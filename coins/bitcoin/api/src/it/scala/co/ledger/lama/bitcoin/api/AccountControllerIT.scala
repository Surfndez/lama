package co.ledger.lama.bitcoin.api

import cats.effect.{IO, Resource}
import cats.implicits._
import co.ledger.lama.bitcoin.api.ConfigSpec.ConfigSpec
import co.ledger.lama.bitcoin.api.models.accountManager._
import co.ledger.lama.bitcoin.api.routes.ValidationResult
import co.ledger.lama.bitcoin.api.{models => apiModels}
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.common.models.Notification.BalanceUpdated
import co.ledger.lama.common.models.Status.{Deleted, Published, Registered, Synchronized}
import co.ledger.lama.common.models.{BalanceUpdatedNotification, Sort}
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import co.ledger.lama.common.utils.{IOAssertion, IOUtils}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{AMQPChannel, ExchangeName, QueueName, RoutingKey}
import fs2.Stream
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.http4s._
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.blaze.BlazeClientBuilder
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class AccountControllerIT_Btc extends AccountControllerIT {
  runTests("/test-accounts-btc.json")
}

class AccountControllerIT_BtcTestnet extends AccountControllerIT {
  runTests("/test-accounts-btc_testnet.json")
}

class AccountControllerIT_Ltc extends AccountControllerIT {
  runTests("/test-accounts-ltc.json")
}

trait AccountControllerIT extends AnyFlatSpecLike with Matchers {

  val conf      = ConfigSource.default.loadOrThrow[ConfigSpec]
  val serverUrl = s"http://${conf.server.host}:${conf.server.port}"
  val accounts  = Uri.unsafeFromString(serverUrl) / "accounts"

  private def accountsRes(resourceName: String): Resource[IO, List[TestAccount]] =
    Resource
      .fromAutoCloseable(
        IO(scala.io.Source.fromFile(getClass.getResource(resourceName).getFile))
      )
      .evalMap { bf =>
        IO.fromEither(decode[List[TestAccount]](bf.getLines().mkString))
      }

  private val accountRegisteringRequest =
    Request[IO](method = Method.POST, uri = accounts)

  private def accountUpdateRequest(accountId: UUID) =
    Request[IO](method = Method.PUT, uri = accounts / accountId.toString)

  private def getAccountRequest(accountId: UUID) =
    Request[IO](
      method = Method.GET,
      uri = accounts / accountId.toString
    )

  private def getOperationsRequest(
      accountId: UUID,
      sort: Sort,
      limit: Int,
      cursor: Option[String]
  ) =
    Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(
        s"$serverUrl/accounts/$accountId/operations?sort=$sort&limit=$limit"
          ++ cursor.map(c => s"&cursor=$c").getOrElse("")
      )
    )

  private def removeAccountRequest(accountId: UUID) =
    Request[IO](
      method = Method.DELETE,
      uri = accounts / accountId.toString
    )

  private def getUTXOsRequest(accountId: UUID, offset: Int, limit: Int, sort: Sort) =
    Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(
        s"$serverUrl/accounts/$accountId/utxos?limit=$limit&offset=$offset&sort=$sort"
      )
    )

  private def getBalancesHistoryRequest(accountId: UUID) =
    Request[IO](
      method = Method.GET,
      uri = accounts / accountId.toString / "balances"
    )

  private def getOperation(accountid: UUID, operationId: Operation.UID) =
    Request[IO](
      method = Method.GET,
      uri = accounts / accountid.toString / "operations" / operationId.hex
    )

  private def validateAddresses(accountId: UUID, addresses: List[String]) =
    Request[IO](
      method = Method.POST,
      uri = accounts / accountId.toString / "recipients"
    ).withEntity(addresses.asJson)

  def runTests(resourceName: String): Seq[Unit] = IOAssertion {

    val resources = for {
      client       <- BlazeClientBuilder[IO](ExecutionContext.global).resource
      inputs       <- accountsRes(resourceName)
      rabbitClient <- RabbitUtils.createClient(conf.eventsConfig.rabbit)
      channel      <- rabbitClient.createConnectionChannel
    } yield (
      inputs,
      client,
      rabbitClient,
      channel
    )

    resources
      .use { case (accounts, client, rabbitClient, channel) =>
        accounts.traverse { account =>
          for {

            // This is retried because sometimes, the keychain service isn't ready when the tests start
            accountRegistered <- IOUtils.retry[RegisterAccountResponse](
              client.expect[RegisterAccountResponse](
                accountRegisteringRequest.withEntity(account.registerRequest)
              )
            )

            coinConf = conf.eventsConfig.coins(account.registerRequest.coin)

            qName <- AccountNotifications
              .ephemeralBoundQueue(
                rabbitClient,
                conf.eventsConfig.lamaEventsExchangeName,
                RoutingKey(
                  s"*.${coinConf.coinFamily}.${coinConf.coin}.${accountRegistered.accountId.toString}"
                )
              )(channel)

            notifications <- AccountNotifications
              .balanceUpdatedNotifications(rabbitClient, qName)(channel)

            accountInfoAfterRegister <- client
              .expect[AccountWithBalance](
                getAccountRequest(accountRegistered.accountId)
              )

            balanceNotification <- AccountNotifications
              .waitBalanceUpdated(
                notifications
              )

            operations <- IOUtils
              .fetchPaginatedItemsByCursor[GetOperationsResult](
                (limit, cursor) =>
                  IOUtils.retryIf[GetOperationsResult](
                    client.expect[GetOperationsResult](
                      getOperationsRequest(
                        accountRegistered.accountId,
                        Sort.Descending,
                        limit,
                        cursor
                      )
                    ),
                    _.operations.nonEmpty
                  ),
                _.cursor.flatMap(_.next),
                20,
                None
              )
              .stream
              .compile
              .toList
              .map(_.flatMap(_.operations))

            firstOperation <- operations.headOption
              .map(o =>
                client
                  .expect[Option[Operation]](getOperation(accountRegistered.accountId, o.uid))
              )
              .getOrElse(IO.none[Operation])

            utxos <- IOUtils
              .fetchPaginatedItems[apiModels.GetUtxosResult](
                (offset, limit) =>
                  client.expect[apiModels.GetUtxosResult](
                    getUTXOsRequest(accountRegistered.accountId, offset, limit, Sort.Ascending)
                  ),
                _.truncated,
                0,
                20
              )
              .stream
              .compile
              .toList
              .map(_.flatMap(_.utxos))

            accountInfoAfterSync <- client.expect[AccountWithBalance](
              getAccountRequest(accountRegistered.accountId)
            )

            balances <- client
              .expect[GetBalanceHistoryResult](
                getBalancesHistoryRequest(
                  accountRegistered.accountId
                )
              )
              .map(_.balances)

            validAddresses = utxos.map(_.address)
            invalidAddress = "invalidAddress"

            recipientsAddressesValidationResult <- client.expect[ValidationResult](
              validateAddresses(accountRegistered.accountId, invalidAddress :: validAddresses)
            )

            accountUpdateStatus <- client.status(
              accountUpdateRequest(accountRegistered.accountId)
                .withEntity(UpdateSyncFrequency(60))
            )

            accountInfoAfterUpdate <- client.expect[AccountWithBalance](
              getAccountRequest(accountRegistered.accountId)
            )

            accountDeletedStatus <- client.status(removeAccountRequest(accountRegistered.accountId))

            deletedAccountResult <- IOUtils.retryIf[AccountWithBalance](
              client.expect[AccountWithBalance](
                getAccountRequest(accountRegistered.accountId)
              ),
              _.lastSyncEvent.exists(_.status == Deleted)
            )
          } yield {
            val accountStr =
              s"Account: ${accountInfoAfterRegister.accountId} (${account.registerRequest.scheme})"

            accountStr should "be registered" in {
              accountInfoAfterRegister.accountId shouldBe accountRegistered.accountId
              accountInfoAfterRegister.lastSyncEvent
                .map(_.status) should (contain(Registered) or contain(Published))
              accountInfoAfterRegister.label shouldBe account.registerRequest.label

              accountRegistered.extendedPublicKey shouldBe account.expected.extendedPublicKey
            }

            it should s"have a balance of ${account.expected.balance}" in {
              accountInfoAfterSync.balance shouldBe BigInt(account.expected.balance)
              accountInfoAfterSync.lastSyncEvent.map(_.status) should contain(Synchronized)
            }

            it should s"emit a balance notification of ${balances.last.balance}" in {
              balanceNotification.account.id shouldBe accountRegistered.accountId
              val Right(notificationBalance) =
                balanceNotification.currentBalance.as[CurrentBalance]
              notificationBalance.balance shouldBe balances.last.balance
            }

            it should s"have ${account.expected.utxosSize} utxos in AccountInfo API" in {
              accountInfoAfterSync.utxos shouldBe account.expected.utxosSize
            }

            it should s"have ${account.expected.amountReceived} amount received" in {
              accountInfoAfterSync.received shouldBe account.expected.amountReceived
            }

            it should s"have ${account.expected.amountSent} amount spent" in {
              accountInfoAfterSync.sent shouldBe account.expected.amountSent
            }

            it should "have a correct balance history" in {
              balances should have size account.expected.balanceHistorySize + 1 //This 1 is mempool balance
              balances.last.balance shouldBe accountInfoAfterSync.balance
            }

            it should s"have ${account.expected.utxosSize} utxos in UTXO API" in {
              utxos.size shouldBe account.expected.utxosSize
            }

            it should s"have only utxos with >0 height and confirmations in UTXO API" in {
              utxos.size shouldBe account.expected.utxosSize
              if (account.expected.utxosSize > 0) {
                utxos.map(_.height).forall(_ > 0) should be(true)
                utxos.map(_.confirmations).forall(_ > 0) should be(true)
              }
            }

            it should s"have ${account.expected.opsSize} operations" in {
              operations.size shouldBe account.expected.opsSize
            }

            val lastTxHash = operations.head.hash
            it should s"have fetch operations to last cursor ${account.expected.lastTxHash}" in {
              lastTxHash shouldBe account.expected.lastTxHash
            }

            it should "have its operations accessible by uid" in {
              firstOperation shouldBe operations.headOption
            }

            it should "be updated" in {
              accountUpdateStatus.code shouldBe 200
              accountInfoAfterUpdate.syncFrequency shouldBe 60
            }

            it should "able to transfer to recipients addresses" in {
              recipientsAddressesValidationResult.valid shouldBe validAddresses
              recipientsAddressesValidationResult.invalid.keys should contain only invalidAddress
            }

            it should "be unregistered" in {
              accountDeletedStatus.code shouldBe 200
              deletedAccountResult.lastSyncEvent.map(_.status) should contain(Deleted)
            }
          }
        }
      }
  }
}

object AccountNotifications {

  def ephemeralBoundQueue(
      rabbitClient: RabbitClient[IO],
      exchangeName: ExchangeName,
      routingKey: RoutingKey
  )(implicit channel: AMQPChannel): IO[QueueName] =
    for {
      q <- rabbitClient.declareQueue
      _ <- rabbitClient.bindQueue(q, exchangeName, routingKey)
    } yield q

  def balanceUpdatedNotifications(
      rabbitClient: RabbitClient[IO],
      queueName: QueueName
  )(implicit channel: AMQPChannel): IO[Stream[IO, BalanceUpdatedNotification]] = {
    RabbitUtils.consume[BalanceUpdatedNotification](rabbitClient, queueName)
  }

  def waitBalanceUpdated(
      notifications: Stream[IO, BalanceUpdatedNotification]
  ): IO[BalanceUpdatedNotification] =
    notifications
      .find(_.status == BalanceUpdated)
      .timeout(1.minute)
      .take(1)
      .compile
      .lastOrError

}
