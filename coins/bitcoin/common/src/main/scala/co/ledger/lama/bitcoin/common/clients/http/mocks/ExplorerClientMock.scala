package co.ledger.lama.bitcoin.common.clients.http.mocks

import cats.effect.{ContextShift, IO, Timer}
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient.Address
import co.ledger.lama.bitcoin.common.models.explorer._
import co.ledger.lama.bitcoin.common.models.transactor.FeeInfo
import fs2.Stream

class ExplorerClientMock(
    blockchain: Map[Address, List[ConfirmedTransaction]] = Map.empty,
    mempool: Map[Address, List[UnconfirmedTransaction]] = Map.empty
) extends ExplorerClient {

  val blocks = blockchain.values.flatten.map(_.block).toList.sorted

  var getConfirmedTransactionsCount: Int   = 0
  var getUnConfirmedTransactionsCount: Int = 0

  def getCurrentBlock: IO[Block] = IO.pure(blockchain.values.flatten.map(_.block).max)

  def getBlock(hash: String): IO[Option[Block]] = IO.pure(blocks.find(_.hash == hash))

  def getBlock(height: Long): IO[Block] = IO.pure(blocks.find(_.height == height).get)

  def getUnconfirmedTransactions(
      addresses: Set[Address]
  )(implicit cs: ContextShift[IO], t: Timer[IO]): Stream[IO, UnconfirmedTransaction] = {
    getUnConfirmedTransactionsCount += 1
    Stream.emits(addresses.flatMap(mempool.get).flatten.toSeq)
  }

  def getConfirmedTransactions(addresses: Seq[String], blockHash: Option[String])(implicit
      cs: ContextShift[IO],
      t: Timer[IO]
  ): fs2.Stream[IO, ConfirmedTransaction] = {
    getConfirmedTransactionsCount += 1
    Stream.emits(addresses.flatMap(blockchain.get).flatten)
  }

  def getSmartFees: IO[FeeInfo] = {
    IO(FeeInfo(500, 1000, 1500))
  }

  override def broadcastTransaction(tx: String): IO[String] = ???

}
