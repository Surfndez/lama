package co.ledger.lama.bitcoin.worker.services

import cats.effect.IO
import co.ledger.lama.bitcoin.common.clients.grpc.KeychainClient
import co.ledger.lama.bitcoin.common.models.interpreter.{AccountAddress, ChangeType}
import co.ledger.lama.bitcoin.worker.services.Keychain.{Address, KeychainId, addressesRanges}
import fs2.Stream
import java.util.UUID

import co.ledger.lama.common.logging.{ContextLogging, DefaultContextLogging, LamaLogContext}

class Keychain(client: KeychainClient) extends ContextLogging {

  def markAsUsed(id: KeychainId, addresses: Set[Address]): IO[Unit] =
    client
      .markAddressesAsUsed(id, addresses.toList)

  def discoverAddresses(
      id: KeychainId,
      change: Option[ChangeType] = None,
      from: Int = 0
  )(implicit lc: LamaLogContext): Stream[IO, List[AccountAddress]] = {
    Stream
      .eval(client.getKeychainInfo(id))
      .flatMap(i => addressesRanges(size = i.lookaheadSize, start = from))
      .evalMap(r =>
        for {
          newAddresses <- client.getAddresses(id, r.head, r.last + 1, change)
          _ <- log.info(
            s"Discovering ${newAddresses.size} new ${change.map(_.name + " ").getOrElse("")}addresses from index : ${r.head}"
          )
        } yield newAddresses
      )
  }

  def knownAddresses(
      id: KeychainId,
      change: Option[ChangeType] = None
  )(implicit lc: LamaLogContext): Stream[IO, List[AccountAddress]] =
    Stream.eval(for {
      knownAddresses <- client.getKnownAddresses(id, change)
      _ <- log.info(
        s"Number of known ${change.map(_.name + " ").getOrElse("")}addresses found : ${knownAddresses.size}"
      )
    } yield knownAddresses)

}

object Keychain extends ContextLogging with DefaultContextLogging {
  type Address    = String
  type KeychainId = UUID

  def addressesRanges[F[_]](size: Int, start: Int = 0): Stream[F, Range] = {
    val from = Stream.iterate(start)(_ + size)
    val to   = Stream.iterate(start + size)(_ + size)

    from
      .zip(to)
      .map(r => r._1 until r._2)
  }
}
