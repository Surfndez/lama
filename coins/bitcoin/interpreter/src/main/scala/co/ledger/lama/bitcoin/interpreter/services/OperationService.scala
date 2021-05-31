package co.ledger.lama.bitcoin.interpreter.services

import cats.data.{NonEmptyList, OptionT}
import cats.effect.{ContextShift, IO}
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.interpreter.models.OperationToSave
import co.ledger.lama.bitcoin.interpreter.services.OperationQueries.{
  OpWithoutDetails,
  OperationDetails
}
import co.ledger.lama.common.logging.ContextLogging
import co.ledger.lama.common.models.{PaginationToken, Sort, TxHash}
import doobie._
import doobie.implicits._
import fs2._
import java.util.UUID

class OperationService(
    db: Transactor[IO]
) extends ContextLogging {

  private val numberOfOperationsToBuildByQuery = 5

  def getOperations(
      accountId: UUID,
      limit: Int,
      sort: Sort,
      cursor: Option[PaginationToken[OperationPaginationState]]
  )(implicit cs: ContextShift[IO]): IO[GetOperationsResult] = {
    for {
      operations <-
        Stream
          .evalSeq(OperationQueries.fetchOperations(accountId, limit, sort, cursor))
          .groupAdjacentBy(_.op.hash) // many operations by hash (RECEIVED AND SENT)
          .chunkN(numberOfOperationsToBuildByQuery)
          .flatMap { ops =>
            val txHashes = ops.map { case (txHash, _) => txHash }.toNel

            val inputsAndOutputs = Stream
              .emits(txHashes.toList)
              .flatMap(txHashes =>
                OperationQueries
                  .fetchOperationDetails(accountId, sort, txHashes)
              )

            Stream
              .chunk(ops)
              .covary[ConnectionIO]
              .zip(inputsAndOutputs)
          }
          .transact(db)
          .through(makeOperation)
          .compile
          .toList

      total <- OperationQueries
        .countOperations(accountId)
        .transact(db)

      // Build previous pagination by taking the first operation state.
      previousPagination <- operations.headOption
        .map { o =>
          OperationQueries
            .hasPreviousPage(accountId, o.uid, sort)
            .transact(db)
            .map {
              case true =>
                Some(
                  PaginationToken(
                    OperationPaginationState(o.uid, o.blockHeight.getOrElse(0)),
                    isNext = false
                  )
                )
              case false => None
            }
        }
        .getOrElse(IO.pure(None))

      // Build next pagination by taking the last operation state
      nextPagination <- operations.lastOption
        .map { o =>
          OperationQueries
            .hasNextPage(accountId, o.uid, sort)
            .transact(db)
            .map {
              case true =>
                Some(
                  PaginationToken(
                    OperationPaginationState(o.uid, o.blockHeight.getOrElse(0)),
                    isNext = true
                  )
                )
              case false => None
            }
        }
        .getOrElse(IO.pure(None))

    } yield {
      // Build the cursor pagination only if we have a previous or next pagination.
      val cursorPagination =
        Option
          .when(previousPagination.isDefined || nextPagination.isDefined) {
            PaginationCursor(
              previous = previousPagination.map(_.toBase64),
              next = nextPagination.map(_.toBase64)
            )
          }

      GetOperationsResult(operations, total, cursorPagination)
    }
  }

  private lazy val makeOperation: Pipe[
    IO,
    (
        (TxHash, Chunk[OpWithoutDetails]),
        OperationQueries.OperationDetails
    ),
    Operation
  ] =
    _.flatMap {
      case (
            (txHash, sentAndReceivedOperations),
            inputsWithOutputsByTxHash
          ) =>
        Stream
          .chunk(sentAndReceivedOperations)
          .takeWhile(_ => txHash == inputsWithOutputsByTxHash.txHash)
          .map { op =>
            operation(op, inputsWithOutputsByTxHash)
          }
    }

  def getOperation(
      accountId: Operation.AccountId,
      operationId: Operation.UID
  )(implicit cs: ContextShift[IO]): IO[Option[Operation]] = {

    val o = for {
      opWithTx <- OptionT(OperationQueries.findOperation(accountId, operationId))
      inputsWithOutputsWithTxHash <- OptionT(
        OperationQueries
          .fetchOperationDetails(
            accountId.value,
            Sort.Ascending,
            NonEmptyList.one(opWithTx.op.hash)
          )
          .compile
          .last
      )
      if inputsWithOutputsWithTxHash.txHash == opWithTx.op.hash
    } yield operation(
      opWithTx,
      inputsWithOutputsWithTxHash
    )

    o.value.transact(db)
  }

  private def operation(
      emptyOperation: OpWithoutDetails,
      inputsWithOutputsByTxHash: OperationDetails
  ) =
    Operation(
      uid = emptyOperation.op.uid,
      accountId = emptyOperation.op.accountId,
      hash = emptyOperation.op.hash.hex,
      transaction = TransactionView(
        id = emptyOperation.tx.id,
        hash = emptyOperation.tx.hash.hex,
        receivedAt = emptyOperation.tx.receivedAt,
        lockTime = emptyOperation.tx.lockTime,
        fees = emptyOperation.tx.fees,
        inputs = inputsWithOutputsByTxHash.inputs,
        outputs = inputsWithOutputsByTxHash.outputs,
        block = emptyOperation.tx.block,
        confirmations = emptyOperation.tx.confirmations
      ),
      operationType = emptyOperation.op.operationType,
      amount = emptyOperation.op.amount,
      fees = emptyOperation.op.fees,
      time = emptyOperation.op.time,
      blockHeight = emptyOperation.op.blockHeight
    )

  def deleteUnconfirmedOperations(accountId: UUID): IO[Int] =
    OperationQueries
      .deleteUnconfirmedOperations(accountId)
      .transact(db)

  def getUtxos(
      accountId: UUID,
      sort: Sort,
      limit: Int,
      offset: Int
  ): IO[GetUtxosResult] =
    for {
      utxos <- OperationQueries
        .fetchConfirmedUTXOs(accountId, sort, Some(limit + 1), Some(offset))
        .transact(db)
        .compile
        .toList

      total <- OperationQueries.countUTXOs(accountId).transact(db)

    } yield {
      // We get 1 more than necessary to know if there's more, then we return the correct number
      GetUtxosResult(utxos.slice(0, limit), total, truncated = utxos.size > limit)
    }

  def getUnconfirmedUtxos(accountId: UUID): IO[List[Utxo]] =
    OperationQueries
      .fetchUnconfirmedUTXOs(accountId)
      .transact(db)
      .compile
      .toList

  def removeFromCursor(accountId: UUID, blockHeight: Long): IO[Int] =
    OperationQueries.removeFromCursor(accountId, blockHeight).transact(db)

  def getUncomputedOperations(accountId: UUID) =
    OperationQueries
      .fetchUncomputedTransactionAmounts(accountId)
      .transact(db)

  def saveOperations(operations: List[OperationToSave]) =
    OperationQueries.saveOperations(operations).transact(db)
}
