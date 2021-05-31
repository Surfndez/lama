package co.ledger.lama.bitcoin.interpreter

import cats.data.NonEmptyList
import cats.effect.IO
import co.ledger.lama.bitcoin.common.models.interpreter.{
  InputView,
  Operation,
  OutputView,
  TransactionView
}
import co.ledger.lama.bitcoin.interpreter.models.OperationToSave
import co.ledger.lama.bitcoin.interpreter.services.{
  OperationQueries,
  OperationService,
  TransactionQueries
}
import co.ledger.lama.common.models.{Sort, TxHash}
import doobie.implicits._
import doobie.util.transactor.Transactor
import java.util.UUID

import co.ledger.lama.common.logging.LamaLogContext

object ITUtils {

  def fetchInputAndOutputs(
      db: Transactor[IO],
      accountId: UUID,
      txHash: TxHash
  ): IO[Option[(List[InputView], List[OutputView])]] = {
    TransactionQueries
      .fetchTransactionDetails(accountId, Sort.Descending, NonEmptyList.one(txHash))
      .transact(db)
      .map(io => (io.inputs, io.outputs))
      .compile
      .last
  }

  def fetchOpAndTx(
      db: Transactor[IO],
      accountId: Operation.AccountId,
      operationId: Operation.UID
  ): IO[Option[OperationQueries.OpWithoutDetails]] =
    OperationQueries.findOperation(accountId, operationId).transact(db)

  def saveTx(db: Transactor[IO], transaction: TransactionView, accountId: UUID): IO[Unit] = {
    TransactionQueries
      .saveTransaction(accountId, transaction)
      .transact(db)
      .void
  }

  def fetchOps(db: Transactor[IO], accountId: UUID): IO[List[OperationQueries.OpWithoutDetails]] = {
    OperationQueries
      .fetchOperations(accountId, 1000, Sort.Descending, None)
      .transact(db)
  }

  def saveOp(db: Transactor[IO], operation: OperationToSave): IO[Unit] = {
    OperationQueries
      .saveOperations(List(operation))
      .transact(db)
      .void
  }

  def compute(operationService: OperationService, accountId: UUID)(implicit lc: LamaLogContext) =
    for {
      ops <- operationService
        .getUncomputedOperations(accountId)
        .flatMap(_.computeOperations)
        .compile
        .toList
      saved <- operationService.saveOperations(ops)
    } yield saved

}
