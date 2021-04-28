package co.ledger.lama.bitcoin.interpreter

import cats.data.OptionT
import cats.effect.{Clock, IO}
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.interpreter.Config.Db
import co.ledger.lama.bitcoin.interpreter.models.AccountTxView
import co.ledger.lama.bitcoin.interpreter.services._
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.models._
import io.circe.syntax._
import fs2._
import doobie.Transactor
import java.time.Instant
import java.util.UUID

class Interpreter(
    publish: Notification => IO[Unit],
    db: Transactor[IO],
    maxConcurrent: Int,
    batchConcurrency: Db.BatchConcurrency
)(implicit clock: Clock[IO])
    extends ContextLogging {

  val transactionService = new TransactionService(db, maxConcurrent)
  val operationService   = new OperationService(db, batchConcurrency)
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
      start               <- clock.monotonic
      _                   <- log.info(s"Flagging inputs and outputs belong")
      _                   <- flaggingService.flagInputsAndOutputs(account.id, addresses)
      flaggingEnd         <- clock.monotonic
      _                   <- operationService.deleteUnconfirmedOperations(account.id)
      cleanUnconfirmedEnd <- clock.monotonic

      _ <- log.info(s"Computing operations")
      nbSavedOps <- operationService
        .compute(account.id)
        .through(
          notify(
            account,
            syncId,
            balanceHistoryCount > 0
          )
        )
        .compile
        .foldMonoid

      computeOperationEnd <- clock.monotonic

      _ <- log.info(s"Computing balance history")
      _ <- balanceService.computeNewBalanceHistory(account.id)

      end <- clock.monotonic

      _ <- log.info(s"$nbSavedOps operations saved in ${end - start}ms ")
      _ <- log.info(
        s"flagging: ${flaggingEnd - start}ms, cleaning: ${cleanUnconfirmedEnd - flaggingEnd}ms, compute operations: ${computeOperationEnd - cleanUnconfirmedEnd}ms"
      )

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

  private def notify(
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
