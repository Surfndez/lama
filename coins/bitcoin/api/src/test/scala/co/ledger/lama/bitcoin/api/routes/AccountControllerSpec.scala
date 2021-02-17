package co.ledger.lama.bitcoin.api.routes

import cats.Show
import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import co.ledger.lama.bitcoin.api.models.transactor.BroadcastTransactionRequest
import co.ledger.lama.bitcoin.common.clients.grpc.TransactorClient
import co.ledger.lama.bitcoin.common.models.interpreter.Utxo
import co.ledger.lama.bitcoin.common.models.transactor._
import co.ledger.lama.common.clients.grpc.AccountManagerClient
import co.ledger.lama.common.models._
import io.circe.JsonObject
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Method, Request}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID
import scala.language.reflectiveCalls

class AccountControllerSpec extends AnyFlatSpec with Matchers {
  import AccountControllerSpec._

  implicit val sc = IO.contextShift(scala.concurrent.ExecutionContext.global)

  val accoundId = UUID.randomUUID()
  val broadcastRequest = Request[IO](
    Method.POST,
    uri = uri"" / accoundId.toString / "transactions" / "send"
  )

  val broadcastBody = BroadcastTransactionRequest(
    RawTransaction(
      hex = "0x......",
      hash = "......",
      witnessHash = "........",
      utxos = NonEmptyList.one(
        Utxo(
          transactionHash = "0x123456",
          outputIndex = 0,
          value = 0,
          address = "",
          scriptHex = "",
          changeType = None,
          derivation = NonEmptyList.one(1),
          time = Instant.now,
          usedInMempool = false
        )
      )
    ),
    List.empty
  )

  broadcastRequest.show should "trigger a synchronization when the transaction has been broadcasted" in {

    val accountManager = accountManagerClient(
      getAccountInfoResponse = id => IO.delay(validAccount(id)),
      resyncAccountResponse =
        (accountId: UUID) => IO.delay(SyncEventResult(accountId, UUID.randomUUID()))
    )

    val transactor =
      transactorClient(broadcastResponse = raw => IO.delay(broadcastTransaction(raw)))

    assert(accountManager.resyncAccountCount == 0)

    val response =
      AccountController
        .transactionsRoutes(accountManager, transactor)
        .run(
          broadcastRequest.withEntity(broadcastBody)
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
      getAccountInfoResponse = id => IO.delay(validAccount(id)),
      resyncAccountResponse =
        (accountId: UUID) => IO.delay(SyncEventResult(accountId, UUID.randomUUID()))
    )

    val transactor =
      transactorClient(
        broadcastResponse = _ => IO.raiseError(new IllegalStateException("broadcast failed"))
      )

    assert(accountManager.resyncAccountCount == 0)

    val response =
      AccountController
        .transactionsRoutes(accountManager, transactor)
        .run(
          broadcastRequest.withEntity(broadcastBody)
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

}

object AccountControllerSpec {

  implicit val showRequest: Show[Request[IO]] = Show.show(r => s"${r.method} ${r.uri}")

  def validAccount(id: UUID) =
    new AccountInfo(id, UUID.randomUUID().toString, CoinFamily.Bitcoin, Coin.Btc, 1L, None, None)

  def broadcastTransaction(rawTransaction: RawTransaction) = new BroadcastTransaction(
    hex = rawTransaction.hex,
    hash = rawTransaction.hash,
    witnessHash = rawTransaction.witnessHash
  )

  private def transactorClient(broadcastResponse: RawTransaction => IO[BroadcastTransaction]) = {
    new TransactorClient {
      override def createTransaction(
          accountId: UUID,
          keychainId: UUID,
          coin: Coin,
          coinSelection: CoinSelectionStrategy,
          outputs: List[PrepareTxOutput],
          feeLevel: FeeLevel,
          customFee: Option[Long]
      ): IO[RawTransaction] = ???

      override def generateSignature(
          rawTransaction: RawTransaction,
          privKey: String
      ): IO[List[String]] = ???

      override def broadcastTransaction(
          keychainId: UUID,
          coinId: String,
          rawTransaction: RawTransaction,
          hexSignatures: List[String]
      ): IO[BroadcastTransaction] = broadcastResponse(rawTransaction)
    }
  }

  private def accountManagerClient(
      getAccountInfoResponse: UUID => IO[AccountInfo],
      resyncAccountResponse: UUID => IO[SyncEventResult]
  ) = {
    new AccountManagerClient {

      var resyncAccountCount = 0

      override def registerAccount(
          keychainId: UUID,
          coinFamily: CoinFamily,
          coin: Coin,
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

      override def getAccounts(limit: Option[Int], offset: Option[Int]): IO[AccountsResult] = ???

      override def getSyncEvents(
          accountId: UUID,
          limit: Option[Int],
          offset: Option[Int],
          sort: Option[Sort]
      ): IO[SyncEventsResult[JsonObject]] = ???
    }
  }

}
