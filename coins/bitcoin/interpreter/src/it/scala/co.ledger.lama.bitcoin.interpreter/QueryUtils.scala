package co.ledger.lama.bitcoin.interpreter

import java.util.UUID

import cats.effect.IO
import co.ledger.lama.bitcoin.common.models.interpreter.{Operation, TransactionView}
import co.ledger.lama.bitcoin.interpreter.models.OperationToSave
import co.ledger.lama.bitcoin.interpreter.services.{OperationQueries, TransactionQueries}
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2.Chunk

object QueryUtils {

  def fetchTx(db: Transactor[IO], accountId: UUID, hash: String): IO[Option[TransactionView]] = {
    OperationQueries
      .fetchTransaction(accountId, hash)
      .transact(db)
  }

  def saveTx(db: Transactor[IO], transaction: TransactionView, accountId: UUID): IO[Unit] = {
    TransactionQueries
      .saveTransaction(transaction, accountId)
      .transact(db)
      .void
  }

  def saveUnconfirmedTxs(
      db: Transactor[IO],
      accountId: UUID,
      transactions: List[TransactionView]
  ): IO[Unit] = {
    TransactionQueries
      .saveUnconfirmedTransactions(accountId, transactions)
      .transact(db)
      .void
  }

  def saveUnconfirmedTxView(
      db: Transactor[IO],
      accountId: UUID,
      transactions: List[TransactionView]
  ): IO[Unit] = {
    OperationQueries
      .saveUnconfirmedTransactionView(accountId, transactions)
      .transact(db)
      .void
  }

  def fetchOps(db: Transactor[IO], accountId: UUID): IO[List[Operation]] = {
    OperationQueries
      .fetchOperations(accountId)
      .transact(db)
      .compile
      .toList
  }

  def saveOp(db: Transactor[IO], operation: OperationToSave): IO[Unit] = {
    OperationQueries
      .saveOperations(Chunk(operation))
      .transact(db)
      .void
  }

}
