package co.ledger.lama.bitcoin.worker

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO, Timer}
import co.ledger.lama.bitcoin.common.clients.grpc.KeychainClient
import co.ledger.lama.bitcoin.common.clients.grpc.mocks.InterpreterClientMock
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient.Address
import co.ledger.lama.bitcoin.common.clients.http.mocks.ExplorerClientMock
import co.ledger.lama.bitcoin.common.models.explorer.{
  DefaultInput,
  Input,
  Output,
  UnconfirmedTransaction
}
import co.ledger.lama.bitcoin.common.models.interpreter.{AccountAddress, ChangeType}
import co.ledger.lama.bitcoin.common.models.keychain.{AccountKey, KeychainInfo}
import co.ledger.lama.bitcoin.common.models.{BitcoinNetwork, Scheme}
import co.ledger.lama.bitcoin.worker.services.{Bookkeeper, Keychain}
import co.ledger.lama.common.models.Coin
import co.ledger.lama.common.models.Coin.Btc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.Random

class BookkeeperSpec extends AnyFlatSpec with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val t: Timer[IO]         = IO.timer(ExecutionContext.global)

  def explorerClient(
      mempool: Map[Address, List[UnconfirmedTransaction]]
  ): Coin => ExplorerClient =
    _ => new ExplorerClientMock(blockchain = Map.empty, mempool)
  val accountId             = UUID.randomUUID()
  val keychainId            = UUID.randomUUID()
  val usedAndFreshAddresses = LazyList.from(1).map(_.toString).take(40)

  "Bookkeeper.recordUnconfirmedTransactions" should "return the currently used addresses of the mempool by an account" in {

    val bookkeeper = new Bookkeeper(
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
      new InterpreterClientMock,
      maxTxsToSavePerBatch = 100,
      maxConcurrent = 1
    )

    val batchResults = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    batchResults should have size 1
    batchResults.head.addresses.map(_.accountAddress) should be(List("11", "12", "13"))
  }

  it should "add all transactions referencing an account's address" in {

    val bookkeeper = usedAndFreshAddresses.drop(10) match {
      case a +: b +: c +: _ =>
        Map(
          a -> List(TransactionFixture.transfer(fromAddress = a)),
          b -> List(TransactionFixture.receive(toAddress = b)),
          c -> List(TransactionFixture.transfer(fromAddress = c))
        )
    }

    val crawler = new Bookkeeper(
      new Keychain(KeychainFixture.keychainClient(usedAndFreshAddresses)),
      explorerClient(bookkeeper),
      new InterpreterClientMock,
      maxTxsToSavePerBatch = 100,
      maxConcurrent = 1
    )

    val batchResults = crawler
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    batchResults should have size 1
    batchResults.head.transactions should contain only (bookkeeper.values.toList.flatten: _*)
  }

  it should "mark all met addresses as used in the keychain" in {

    val keychainClient = KeychainFixture.keychainClient(usedAndFreshAddresses)
    val keychain       = new Keychain(keychainClient)

    val bookkeeper = new Bookkeeper(
      keychain,
      explorerClient(
        usedAndFreshAddresses
          .slice(10, 17)
          .map { address =>
            address -> List(
              TransactionFixture.transfer(fromAddress = address)
            )
          }
          .toMap
      ),
      new InterpreterClientMock,
      maxTxsToSavePerBatch = 100,
      maxConcurrent = 1
    )

    val _ = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    keychainClient.newlyMarkedAddresses.toList should contain only ((11 to 17).map(_.toString): _*)
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

    val bookkeeper = new Bookkeeper(
      new Keychain(KeychainFixture.keychainClient(usedAndFreshAddresses)),
      explorerClient(transactions),
      interpreter,
      maxTxsToSavePerBatch = 100,
      maxConcurrent = 1
    )

    val _ = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    val expectedSavedTransactions = transactions.values.flatten

    interpreter.savedUnconfirmedTransactions should have size 1
    interpreter.savedUnconfirmedTransactions.head._1 shouldBe accountId
    interpreter.savedUnconfirmedTransactions.head._2 should contain only (expectedSavedTransactions.toSeq: _*)
  }

  it should "be empty when no matching transaction found" in {

    val bookkeeper = new Bookkeeper(
      new Keychain(KeychainFixture.keychainClient(usedAndFreshAddresses)),
      explorerClient(
        mempool = Map.empty
      ),
      new InterpreterClientMock,
      maxTxsToSavePerBatch = 100,
      maxConcurrent = 1
    )

    val batches = bookkeeper
      .record[UnconfirmedTransaction](
        Btc,
        accountId,
        keychainId,
        None
      )
      .compile
      .toList
      .unsafeRunSync()

    batches shouldBe Seq()
  }

}

private object TransactionFixture {

  def output(address: Address) = Output(
    outputIndex = 0,
    value = 1L,
    address,
    scriptHex = ""
  )

  def input(address: Address): Input = DefaultInput(
    outputHash = "",
    outputIndex = 0,
    inputIndex = 0,
    value = 1L,
    address = address,
    scriptSignature = "",
    txinwitness = List.empty,
    sequence = 0L
  )

  def transfer(fromAddress: Address): UnconfirmedTransaction =
    unconfirmedTransaction(
      inputs = NonEmptyList.one(input(fromAddress)),
      outputs = NonEmptyList.one(output(s"dest-${Random.nextInt(8)})"))
    )

  def receive(toAddress: Address): UnconfirmedTransaction =
    unconfirmedTransaction(
      inputs = NonEmptyList.one(input(s"sender-${Random.nextInt(8)}")),
      outputs = NonEmptyList.one(output(toAddress))
    )

  def unconfirmedTransaction(
      inputs: NonEmptyList[Input],
      outputs: NonEmptyList[Output]
  ): UnconfirmedTransaction = UnconfirmedTransaction(
    id = s"id-${Random.nextInt(100)}",
    hash = s"hash${Random.nextInt(6)}",
    receivedAt = Instant.now(),
    lockTime = 0L,
    fees = 1,
    inputs = inputs.toList,
    outputs = outputs.toList,
    confirmations = 1
  )

}

object KeychainFixture {

  trait UsedAddressesTracker {
    val newlyMarkedAddresses: mutable.ArrayDeque[Address] = mutable.ArrayDeque.empty
  }

  def keychainClient(
      addresses: LazyList[Address],
      lookaheadSize: Int = 20
  ): KeychainClient with UsedAddressesTracker =
    new KeychainClient with UsedAddressesTracker {

      override def create(
          accountKey: AccountKey,
          scheme: Scheme,
          lookaheadSize: Int,
          network: BitcoinNetwork
      ): IO[KeychainInfo] = ???

      override def getKeychainInfo(keychainId: UUID): IO[KeychainInfo] =
        IO.delay(
          KeychainInfo(
            keychainId,
            externalDescriptor = "externalDesc",
            internalDescriptor = "internalDesc",
            extendedPublicKey = "extendedPublicKey",
            slip32ExtendedPublicKey = "slip32ExtendedPublicKey",
            lookaheadSize = lookaheadSize,
            scheme = Scheme.Bip44,
            network = BitcoinNetwork.MainNet
          )
        )

      override def getAddresses(
          keychainId: UUID,
          fromIndex: Int,
          toIndex: Int,
          changeType: Option[ChangeType]
      ): IO[List[AccountAddress]] =
        IO.delay(
          addresses
            .slice(fromIndex, toIndex)
            .map(AccountAddress(_, ChangeType.External, derivation = NonEmptyList.one(1)))
            .toList
        )

      override def markAddressesAsUsed(keychainId: UUID, addresses: List[String]): IO[Unit] =
        IO.delay(newlyMarkedAddresses.addAll(addresses))

      override def getFreshAddresses(
          keychainId: UUID,
          change: ChangeType,
          size: Int
      ): IO[List[AccountAddress]] = ???

      override def getAddressesPublicKeys(
          keychainId: UUID,
          derivations: NonEmptyList[NonEmptyList[Int]]
      ): IO[List[String]] = ???

      override def deleteKeychain(keychainId: UUID): IO[Unit] = ???
    }

}
