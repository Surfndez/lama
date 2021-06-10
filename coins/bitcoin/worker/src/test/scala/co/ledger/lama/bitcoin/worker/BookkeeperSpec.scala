package co.ledger.lama.bitcoin.worker

import java.time.Instant

import cats.effect.{ContextShift, IO, Timer}
import co.ledger.lama.bitcoin.common.clients.grpc.mocks.InterpreterClientMock
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient.Address
import co.ledger.lama.bitcoin.common.clients.http.mocks.ExplorerClientMock
import co.ledger.lama.bitcoin.common.models.explorer.{
  Block,
  ConfirmedTransaction,
  UnconfirmedTransaction
}
import co.ledger.lama.bitcoin.common.models.interpreter.{AccountAddress, ChangeType}
import co.ledger.lama.bitcoin.worker.services.{Bookkeeper, Keychain}
import co.ledger.lama.common.models.Coin
import co.ledger.lama.common.models.Coin.Btc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.util.UUID

import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.common.utils.IOAssertion
import fs2.Stream

import scala.concurrent.ExecutionContext

class BookkeeperSpec extends AnyFlatSpec with Matchers with DefaultContextLogging {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val t: Timer[IO]         = IO.timer(ExecutionContext.global)

  def explorerClient(
      mempool: Map[Address, List[UnconfirmedTransaction]] = Map.empty,
      blockchain: Map[Address, List[ConfirmedTransaction]] = Map.empty
  ): Coin => ExplorerClient =
    _ => new ExplorerClientMock(blockchain, mempool)
  val accountId             = UUID.randomUUID()
  val keychainId            = UUID.randomUUID()
  val usedAndFreshAddresses = LazyList.from(1).map(_.toString).take(150)

  "Bookkeeper.recordUnconfirmedTransactions" should "return the currently used addresses of the mempool by an account" in {

    val bookkeeper = Bookkeeper(
      new Keychain(KeychainFixture.keychainClient(usedAndFreshAddresses)),
      explorerClient(
        mempool = usedAndFreshAddresses
          .slice(10, 13)
          .map { address =>
            address -> List(
              TransactionFixture.transfer(fromAddress = address)
            )
          }
          .toMap
      ),
      new InterpreterClientMock
    )

    val addresses = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        ChangeType.External,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    addresses.map(_.accountAddress) should contain.only("11", "12", "13")
  }

  it should "add all transactions referencing an account's address" in {

    val transactions = usedAndFreshAddresses.drop(10) match {
      case a +: b +: c +: _ =>
        Map(
          a -> List(TransactionFixture.transfer(fromAddress = a)),
          b -> List(TransactionFixture.receive(toAddress = b)),
          c -> List(TransactionFixture.transfer(fromAddress = c))
        )
    }

    val bookkeeper = Bookkeeper(
      new Keychain(KeychainFixture.keychainClient(usedAndFreshAddresses)),
      explorerClient(mempool = transactions),
      new InterpreterClientMock
    )

    val addresses = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        ChangeType.External,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    addresses
      .map(_.accountAddress) should contain only (transactions.keys.toList: _*)
  }

  it should "mark all met addresses as used in the keychain" in {

    val keychainClient = KeychainFixture.keychainClient(usedAndFreshAddresses)
    val keychain       = new Keychain(keychainClient)

    val bookkeeper = Bookkeeper(
      keychain,
      explorerClient(
        mempool = usedAndFreshAddresses
          .slice(10, 17)
          .map { address =>
            address -> List(
              TransactionFixture.transfer(fromAddress = address)
            )
          }
          .toMap
      ),
      new InterpreterClientMock
    )

    val _ = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        ChangeType.External,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    keychainClient.newlyMarkedAddresses.keys.toList should contain only ((11 to 17).map(
      _.toString
    ): _*)
  }

  it should "send transactions and corresponding used addresses to the interpreter" in {

    val transactions = usedAndFreshAddresses.drop(10) match {
      case a +: b +: c +: _ =>
        Map(
          a -> List(TransactionFixture.transfer(fromAddress = a)),
          b -> List(TransactionFixture.receive(toAddress = b)),
          c -> List(TransactionFixture.transfer(fromAddress = c))
        )
    }

    val interpreter = new InterpreterClientMock

    val bookkeeper = Bookkeeper(
      new Keychain(KeychainFixture.keychainClient(usedAndFreshAddresses)),
      explorerClient(mempool = transactions),
      interpreter
    )

    val _ = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        ChangeType.External,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    val expectedSavedTransactions = transactions.values.flatten.map(_.toTransactionView)

    interpreter.savedUnconfirmedTransactions should have size 1
    interpreter.savedUnconfirmedTransactions.head._1 shouldBe accountId
    interpreter.savedUnconfirmedTransactions.head._2 should contain only (expectedSavedTransactions.toSeq: _*)
  }

  it should "be empty when no matching transaction found" in {

    val bookkeeper = Bookkeeper(
      new Keychain(KeychainFixture.keychainClient(usedAndFreshAddresses)),
      explorerClient(),
      new InterpreterClientMock
    )

    val addresses = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        ChangeType.External,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    addresses shouldBe List.empty[AccountAddress]
  }

  it should "always try all known addresses" in IOAssertion {

    val keychain = new Keychain(KeychainFixture.keychainClient(usedAndFreshAddresses))

    for {

      // get the first 100 addresses
      addressesToUse <- keychain
        .discoverAddresses(keychainId)
        .take(5)
        .flatMap(addressList => Stream.emits(addressList.map(_.accountAddress)))
        .compile
        .toList

      // mark them as used
      _ = keychain.markAsUsed(
        keychainId,
        addressesToUse.toSet
      )

      //create a transaction from the ~90th address
      addressUsed = addressesToUse.takeRight(10).head
      transaction = Map(
        addressUsed -> List(
          TransactionFixture.confirmed.transfer(addressUsed, Block("hash", 1L, Instant.now()))
        )
      )

      bookkeeper = Bookkeeper(
        keychain,
        explorerClient(
          blockchain = transaction
        ),
        new InterpreterClientMock
      )

      addressesMatched <- bookkeeper
        .record[ConfirmedTransaction](
          Btc,
          accountId,
          keychainId,
          ChangeType.External,
          None
        )
        .compile
        .toList

    } yield {
      addressesMatched.map(_.accountAddress).head shouldBe addressUsed
    }

  }

}
