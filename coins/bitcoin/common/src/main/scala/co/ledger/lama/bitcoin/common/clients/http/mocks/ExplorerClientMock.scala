package co.ledger.lama.bitcoin.common.clients.http.mocks

import cats.effect.IO
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient.Address
import co.ledger.lama.bitcoin.common.models.explorer._
import co.ledger.lama.bitcoin.common.models.transactor.FeeInfo
import co.ledger.lama.common.logging.LamaLogContext
import fs2.Stream

class ExplorerClientMock(
    blockchain: Map[Address, List[ConfirmedTransaction]] = Map.empty,
    mempool: Map[Address, List[UnconfirmedTransaction]] = Map.empty
) extends ExplorerClient {

  val blocks = blockchain.values.flatten.map(_.block).toList.sorted

  var getConfirmedTransactionsCount: Int   = 0
  var getUnConfirmedTransactionsCount: Int = 0

  def getCurrentBlock(implicit lc: LamaLogContext): IO[Block] =
    IO.pure(blockchain.values.flatten.map(_.block).max)

  def getBlock(hash: String)(implicit lc: LamaLogContext): IO[Option[Block]] =
    IO.pure(blocks.find(_.hash == hash))

  def getBlock(height: Long)(implicit lc: LamaLogContext): IO[Block] =
    IO.pure(blocks.find(_.height == height).get)

  def getUnconfirmedTransactions(
      addresses: Set[Address]
  )(implicit lc: LamaLogContext): Stream[IO, UnconfirmedTransaction] = {
    getUnConfirmedTransactionsCount += 1
    Stream.emits(addresses.flatMap(mempool.get).flatten.toSeq)
  }

  def getConfirmedTransactions(
      addresses: Seq[String],
      blockHash: Option[String]
  )(implicit lc: LamaLogContext): fs2.Stream[IO, ConfirmedTransaction] = {
    getConfirmedTransactionsCount += 1
    Stream.emits(addresses.flatMap(blockchain.get).flatten)
  }

  def getSmartFees(implicit lc: LamaLogContext): IO[FeeInfo] = {
    IO(FeeInfo(500, 1000, 1500))
  }

  override def broadcastTransaction(tx: String)(implicit lc: LamaLogContext): IO[String] = ???

  override def getRawTransactionHex(transactionHash: String)(implicit
      lc: LamaLogContext
  ): IO[String] =
    IO.pure("raw hex for " ++ transactionHash)
}
