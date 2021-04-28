package co.ledger.lama.bitcoin.api.routes

import cats.Show
import cats.data.{NonEmptyList, OptionT}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import co.ledger.lama.bitcoin.api.models.transactor.BroadcastTransactionRequest
import co.ledger.lama.bitcoin.common.clients.grpc.TransactorClient
import co.ledger.lama.bitcoin.common.clients.grpc.TransactorClient.{
  Accepted,
  Address,
  AddressValidation,
  Rejected
}
import co.ledger.lama.bitcoin.common.clients.grpc.mocks.KeychainClientMock
import co.ledger.lama.bitcoin.common.models.interpreter.Utxo
import co.ledger.lama.bitcoin.common.models.transactor._
import co.ledger.lama.common.clients.grpc.AccountManagerClient
import co.ledger.lama.common.models._
import co.ledger.lama.common.utils.IOAssertion
import io.circe.parser._
import io.circe.{Json, JsonObject}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Method, Request}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.language.reflectiveCalls

class AccountControllerSpec extends AnyFlatSpec with Matchers {
  import AccountControllerSpec._

  val accoundId = UUID.randomUUID()
  val broadcastEndpoint = Request[IO](
    Method.POST,
    uri = uri"" / accoundId.toString / "transactions" / "send"
  )

  val broadcastBody = BroadcastTransactionRequest(
    RawTransaction(
      hex = "0x......",
      hash = "......",
      witnessHash = "........"
    ),
    List(List(1)),
    List.empty
  )

  broadcastEndpoint.show should "trigger a synchronization when the transaction has been broadcasted" in {

    val accountManager = accountManagerClient(
      getAccountInfoResponse = validAccount,
      resyncAccountResponse =
        (accountId: UUID) => IO.delay(SyncEventResult(accountId, UUID.randomUUID()))
    )

    val transactor =
      transactorClient(broadcastResponse = raw => IO.delay(broadcastTransaction(raw)))
    val keychainService = new KeychainClientMock

    assert(accountManager.resyncAccountCount == 0)

    val response =
      AccountController
        .transactionsRoutes(keychainService, accountManager, transactor)
        .run(
          broadcastEndpoint.withEntity(broadcastBody)
        )

    response
      .getOrElse(fail("Request was not handled: "))
      .map { response =>
        response.status should be(org.http4s.Status.Ok)

      }
      .unsafeRunSync()

    accountManager.resyncAccountCount shouldBe 1
  }

  it should "not trigger a synchro when the transaction has not been broadcasted" in {

    val accountManager = accountManagerClient(
      getAccountInfoResponse = validAccount,
      resyncAccountResponse =
        (accountId: UUID) => IO.delay(SyncEventResult(accountId, UUID.randomUUID()))
    )

    val transactor =
      transactorClient(
        broadcastResponse = _ => IO.raiseError(new IllegalStateException("broadcast failed"))
      )
    val keychainService = new KeychainClientMock

    assert(accountManager.resyncAccountCount == 0)

    val response =
      AccountController
        .transactionsRoutes(keychainService, accountManager, transactor)
        .run(
          broadcastEndpoint.withEntity(broadcastBody)
        )

    a[IllegalStateException] should be thrownBy {

      response
        .getOrElse(fail("Request was not handled: "))
        .map { response =>
          response.status should be(org.http4s.Status.Ok)

        }
        .unsafeRunSync()
    }

    accountManager.resyncAccountCount shouldBe 0
  }

  val validationEndpoint = Request[IO](
    Method.POST,
    uri = uri"" / accoundId.toString / "recipients"
  )

  validationEndpoint.show should "accept a list of addresses" in IOAssertion {

    val addressesList = json("""
        | [
        |    "address1",
        |    "address2",
        |    "address3"
        | ]
        |
        |""".stripMargin)

    val controller = AccountController.transactionsRoutes(
      keychainClient = new KeychainClientMock,
      accountManagerClient = accountManagerClient(
        getAccountInfoResponse = validAccount,
        resyncAccountResponse = unusedStub
      ),
      transactorClient = transactorClient(
        validateAddressesResponse = allAccepted
      )
    )

    (
      for {
        response <- controller.run(validationEndpoint.withEntity(addressesList))
        body     <- OptionT.liftF(response.as[Json])
      } yield {

        response.status shouldBe org.http4s.Status.Ok

        body shouldBe json(
          """
          | {
          |   "valid" : [ "address1", "address2", "address3" ],
          |   "invalid" : {}
          | }
          |""".stripMargin
        )
      }
    ).value
  }

  it should "refuse an empty list" in {

    val controller = AccountController.transactionsRoutes(
      keychainClient = new KeychainClientMock,
      accountManagerClient = accountManagerClient(
        getAccountInfoResponse = validAccount,
        resyncAccountResponse = unusedStub
      ),
      transactorClient = transactorClient(
        validateAddressesResponse = allAccepted
      )
    )

    a[org.http4s.InvalidMessageBodyFailure] should be thrownBy {
      controller
        .run(validationEndpoint.withEntity(Json.arr()))
        .value
        .unsafeRunSync()
    }
  }

  it should "show also the invalid addresses" in IOAssertion {

    val addresses = json("""
         | [
         |    "address1",
         |    "address2",
         |    "address3"
         | ]
         |
         |""".stripMargin)

    val controller = AccountController.transactionsRoutes(
      keychainClient = new KeychainClientMock,
      accountManagerClient = accountManagerClient(
        getAccountInfoResponse = validAccount,
        resyncAccountResponse = unusedStub
      ),
      transactorClient = transactorClient(
        validateAddressesResponse = firstRejected
      )
    )

    (
      for {
        response <- controller.run(validationEndpoint.withEntity(addresses))
        body     <- OptionT.liftF(response.as[Json])
      } yield {

        response.status shouldBe org.http4s.Status.Ok

        body shouldBe json(
          """
            | {
            |   "valid" : [ "address2", "address3" ],
            |   "invalid" :
            |      {
            |         "address1" : "rejected"
            |      }
            | }
            |""".stripMargin
        )
      }
    ).value
  }

}

object AccountControllerSpec {

  implicit val showRequest: Show[Request[IO]] = Show.show(r => s"${r.method} ${r.uri}")

  def json(j: String): Json = parse(j).getOrElse(Json.Null)

  def unusedStub[A, B]: A => IO[B] = _ => IO.never
  def allAccepted(addresses: List[Address]): IO[List[AddressValidation]] =
    IO.delay(addresses.map(Accepted))
  def firstRejected(addresses: List[Address]): IO[List[AddressValidation]] =
    IO.delay(Rejected(addresses.head, reason = "rejected") :: addresses.tail.map(Accepted))

  def validAccount(identifier: UUID): IO[AccountInfo] =
    IO.delay(
      new AccountInfo(
        Account(identifier.toString, CoinFamily.Bitcoin, Coin.Btc, AccountGroup("TestGroup")),
        1L,
        None,
        None
      )
    )

  def broadcastTransaction(rawTransaction: RawTransaction) = rawTransaction

  private def transactorClient(
      broadcastResponse: RawTransaction => IO[RawTransaction] = unusedStub,
      validateAddressesResponse: List[Address] => IO[List[AddressValidation]] = unusedStub
  ) = {
    new TransactorClient {
      override def createTransaction(
          accountId: UUID,
          keychainId: UUID,
          coin: Coin,
          coinSelection: CoinSelectionStrategy,
          outputs: List[PrepareTxOutput],
          feeLevel: FeeLevel,
          customFeePerKb: Option[Long],
          maxUtxos: Option[Int]
      ): IO[CreateTransactionResponse] = ???

      override def generateSignatures(
          rawTransaction: RawTransaction,
          utxos: List[Utxo],
          privKey: String
      ): IO[List[String]] = ???

      override def broadcastTransaction(
          keychainId: UUID,
          coinId: String,
          rawTransaction: RawTransaction,
          derivations: List[List[Int]],
          hexSignatures: List[String]
      ): IO[RawTransaction] = broadcastResponse(rawTransaction)

      override def validateAddresses(
          coin: Coin,
          addresses: NonEmptyList[Address]
      ): IO[List[AddressValidation]] = validateAddressesResponse(addresses.toList)
    }
  }

  private def accountManagerClient(
      getAccountInfoResponse: UUID => IO[AccountInfo],
      resyncAccountResponse: UUID => IO[SyncEventResult]
  ) = {
    new AccountManagerClient {

      var resyncAccountCount = 0

      override def registerAccount(
          account: Account,
          syncFrequency: Option[Long],
          label: Option[String]
      ): IO[SyncEventResult] = ???

      override def updateSyncFrequency(accountId: UUID, frequency: Long): IO[Unit] = ???

      override def updateLabel(accountId: UUID, label: String): IO[Unit] = ???

      override def updateAccount(accountId: UUID, frequency: Long, label: String): IO[Unit] = ???

      override def resyncAccount(accountId: UUID, wipe: Boolean): IO[SyncEventResult] = {
        resyncAccountCount += 1
        resyncAccountResponse(accountId)
      }

      override def unregisterAccount(accountId: UUID): IO[SyncEventResult] = ???

      override def getAccountInfo(accountId: UUID): IO[AccountInfo] = {
        getAccountInfoResponse(accountId)
      }

      override def getAccounts(
          group: Option[String],
          limit: Int,
          offset: Option[Int]
      ): IO[AccountsResult] = ???

      override def getSyncEvents(
          accountId: UUID,
          limit: Int,
          offset: Option[Int],
          sort: Option[Sort]
      ): IO[SyncEventsResult[JsonObject]] = ???
    }
  }

}
