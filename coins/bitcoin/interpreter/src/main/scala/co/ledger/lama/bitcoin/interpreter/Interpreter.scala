package co.ledger.lama.bitcoin.interpreter

import cats.data.OptionT
import cats.effect.{Clock, ContextShift, IO}
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.interpreter.Config.Db
import co.ledger.lama.bitcoin.interpreter.services._
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.models._
import io.circe.syntax._
import fs2._
import doobie.Transactor
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.interpreter.models.{
  AccountTxView,
  Action,
  Delete,
  Save,
  TransactionAmounts
}

class Interpreter(
    publish: Notification => IO[Unit],
    explorer: Coin => ExplorerClient,
    db: Transactor[IO],
    maxConcurrent: Int,
    batchConcurrency: Db.BatchConcurrency
)(implicit cs: ContextShift[IO], clock: Clock[IO])
    extends ContextLogging {

  val transactionService = new TransactionService(db, maxConcurrent)
  val operationService   = new OperationService(db)
  val flaggingService    = new FlaggingService(db)
  val balanceService     = new BalanceService(db, batchConcurrency)

  def saveTransactions: Pipe[IO, AccountTxView, Int] =
    transactionService.saveTransactions

  def getLastBlocks(
      accountId: UUID
  ): IO[List[BlockView]] = {
    implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)

    log.info(s"Getting last known blocks") *>
      transactionService
        .getLastBlocks(accountId)
        .compile
        .toList
  }

  def getOperations(
      accountId: UUID,
      requestLimit: Int,
      sort: Sort,
      cursor: Option[PaginationToken[OperationPaginationState]]
  ): IO[GetOperationsResult] = {
    implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)

    val limit = if (requestLimit <= 0) 20 else requestLimit
    log.info(s"""Getting operations with parameters:
                  - limit: $limit
                  - sort: $sort
                  - cursor: $cursor""") *>
      operationService.getOperations(accountId, limit, sort, cursor)
  }

  def getOperation(
      accountId: Operation.AccountId,
      operationId: Operation.UID
  ): IO[Option[Operation]] =
    operationService.getOperation(accountId, operationId)

  def getUtxos(
      accountId: UUID,
      requestLimit: Int,
      requestOffset: Int,
      sort: Sort
  ): IO[GetUtxosResult] = {
    implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)

    val limit  = if (requestLimit <= 0) 20 else requestLimit
    val offset = if (requestOffset < 0) 0 else requestOffset
    log.info(s"""Getting UTXOs with parameters:
                               - limit: $limit
                               - offset: $offset
                               - sort: $sort""") *>
      operationService.getUtxos(accountId, sort, limit, offset)
  }

  def getUnconfirmedUtxos(
      accountId: UUID
  ): IO[List[Utxo]] = {
    implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)

    log.info(s"""Getting UTXOs""") *>
      operationService.getUnconfirmedUtxos(accountId)
  }

  def removeDataFromCursor(
      accountId: UUID,
      blockHeight: Long,
      followUpId: UUID
  ): IO[Int] = {
    implicit val lc: LamaLogContext =
      LamaLogContext().withAccountId(accountId).withFollowUpId(followUpId)

    for {
      _     <- log.info(s"""Deleting data with parameters:
                      - blockHeight: $blockHeight""")
      txRes <- transactionService.removeFromCursor(accountId, blockHeight)
      _     <- log.info(s"Deleted $txRes operations")
      balancesRes <- balanceService.removeBalanceHistoryFromCursor(
        accountId,
        blockHeight
      )
      _ <- log.info(s"Deleted $balancesRes balances history")
    } yield txRes
  }

  def compute(
      account: Account,
      syncId: UUID,
      addresses: List[AccountAddress]
  ): IO[Int] = {
    implicit val lc: LamaLogContext = LamaLogContext().withAccount(account).withFollowUpId(syncId)

    for {
      balanceHistoryCount <- balanceService.getBalanceHistoryCount(account.id)
      _                   <- log.info(s"Flagging inputs and outputs belong")
      _                   <- flaggingService.flagInputsAndOutputs(account.id, addresses)
      _                   <- operationService.deleteUnconfirmedOperations(account.id)

      _ <- log.info(s"Computing operations")
      nbSavedOps <- operationService
        .getUncomputedOperations(account.id)
        .evalMap(tx => getAppropriateAction(account, tx))
        .broadcastThrough(
          newOperationPipe(account, syncId, balanceHistoryCount > 0),
          rejectedTransactionPipe(account, syncId)
        )
        .compile
        .foldMonoid
      _ <- log.info(s"$nbSavedOps operations saved")

      _              <- log.info(s"Computing balance history")
      _              <- balanceService.computeNewBalanceHistory(account.id)
      currentBalance <- balanceService.getCurrentBalance(account.id)

      _ <- log.info(s"Notifying computation end with balance $currentBalance")
      _ <- publish(
        BalanceUpdatedNotification(
          account = account,
          syncId = syncId,
          currentBalance = currentBalance.asJson
        )
      )

    } yield nbSavedOps
  }

  private def getAppropriateAction(
      account: Account,
      tx: TransactionAmounts
  )(implicit lc: LamaLogContext): IO[Action] =
    tx.blockHeight match {
      case Some(_) => IO.pure(Save(tx))
      case None =>
        explorer(account.coin).getTransaction(tx.hash).map {
          case Some(_) => Save(tx)
          case None    => Delete(tx)
        }
    }

  private def newOperationPipe(
      account: Account,
      syncId: UUID,
      shouldNotify: Boolean
  )(implicit
      cs: ContextShift[IO],
      clock: Clock[IO],
      lc: LamaLogContext
  ): Pipe[IO, Action, Int] = {
    _.through(saveOperationPipe)
      .through(
        notifyNewOperation(
          account,
          syncId,
          shouldNotify
        )
      )
  }

  private def saveOperationPipe(implicit
      cs: ContextShift[IO],
      clock: Clock[IO],
      lc: LamaLogContext
  ): Pipe[IO, Action, Operation.UID] = {

    val batchSize = Math.max(1000 / batchConcurrency.value, 100)

    in =>
      in.collect { case Save(tx) => tx }
        .flatMap(_.computeOperations)
        .chunkN(batchSize)
        .parEvalMap(batchConcurrency.value) { operations =>
          for {
            start    <- clock.monotonic(TimeUnit.MILLISECONDS)
            savedOps <- operationService.saveOperations(operations.toList)
            end      <- clock.monotonic(TimeUnit.MILLISECONDS)
            _ <- log.debug(
              s"${operations.head.map(_.uid)}: $savedOps operations saved in ${end - start} ms"
            )
          } yield operations.map(_.uid)

        }
        .flatMap(Stream.chunk)
  }

  private def notifyNewOperation(
      account: Account,
      syncId: UUID,
      shouldNotify: Boolean
  ): Pipe[IO, Operation.UID, Int] = {
    _.parEvalMap(maxConcurrent) { opId =>
      OptionT
        .whenF(shouldNotify)(
          operationService.getOperation(Operation.AccountId(account.id), opId)
        )
        .foldF(IO.unit) { operation =>
          publish(
            OperationNotification(
              account = account,
              syncId = syncId,
              operation = operation.asJson
            )
          )
        } *> IO.pure(1)
    }
  }

  private def rejectedTransactionPipe(
      account: Account,
      syncId: UUID
  ): Pipe[IO, Action, Int] = {
    _.through(deleteRejectedTransactionPipe)
      .through(notifyDeleteTransaction(account, syncId))
  }

  private def deleteRejectedTransactionPipe: Pipe[IO, Action, String] = { stream =>
    stream
      .collect { case Delete(tx) => tx }
      .evalMap { tx =>
        transactionService.deleteUnconfirmedTransaction(tx.accountId, tx.hash)
      }
  }

  private def notifyDeleteTransaction(
      account: Account,
      syncId: UUID
  ): Pipe[IO, String, Int] = {
    _.parEvalMap(maxConcurrent) { hash =>
      publish(
        TransactionDeleted(
          account = account,
          syncId = syncId,
          hash = hash
        )
      ) *> IO.pure(1)
    }
  }

  def getBalance(
      accountId: UUID
  ): IO[CurrentBalance] =
    balanceService.getCurrentBalance(accountId)

  def getBalanceHistory(
      accountId: UUID,
      startO: Option[Instant],
      endO: Option[Instant],
      interval: Int
  ): IO[List[BalanceHistory]] = {
    implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)

    if (startO.forall(start => endO.forall(end => start.isBefore(end))) && interval >= 0)
      log.info(s"""Getting balances with parameters:
                       - start: $startO
                       - end: $endO
                       - interval: $interval""") *>
        balanceService.getBalanceHistory(
          accountId,
          startO,
          endO,
          if (interval > 0) Some(interval) else None
        )
    else {
      val e = new Exception(
        "Invalid parameters : 'start' should not be after 'end' and 'interval' should be positive"
      )
      log.error(
        s"""GetBalanceHistory error with parameters :
           start    : $startO
           end      : $endO
           interval : $interval""",
        e
      )
      IO.raiseError(e)
    }
  }

}
