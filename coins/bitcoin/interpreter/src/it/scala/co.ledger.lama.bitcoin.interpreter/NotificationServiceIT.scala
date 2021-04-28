package co.ledger.lama.bitcoin.interpreter

import cats.effect.{IO, Resource}
import co.ledger.lama.common.models._
import co.ledger.lama.common.services.RabbitNotificationService
import co.ledger.lama.common.services.RabbitNotificationService.NotificationPublisher
import co.ledger.lama.common.utils.IOAssertion
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{AMQPChannel, RoutingKey}
import fs2.Stream
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

import java.util.UUID

class NotificationServiceIT extends AnyFlatSpecLike with Matchers {

  def consumeNotification[T <: Notification](consumer: Stream[IO, T]): IO[T] =
    consumer.take(1).compile.last.map(_.get)

  val conf: Config = ConfigSource.default.loadOrThrow[Config]
  val rabbit: Resource[IO, (RabbitClient[IO], AMQPChannel)] = for {
    client  <- RabbitUtils.createClient(conf.rabbit)
    channel <- client.createConnectionChannel
  } yield (client, channel)

  IOAssertion {
    rabbit
      .use { case (rabbitClient, channel) =>
        val notificationPublisher: IO[NotificationPublisher] =
          RabbitNotificationService.publisher(
            conf.lamaNotificationsExchangeName,
            RabbitNotificationService.routingKey
          )(rabbitClient, channel)

        def routedNotifications(routingKey: RoutingKey)(implicit channel: AMQPChannel) =
          for {
            qName <- rabbitClient.declareQueue(channel)
            _     <- rabbitClient.bindQueue(qName, conf.lamaNotificationsExchangeName, routingKey)
            consumer <- RabbitUtils
              .consume[OperationsComputedNotification](rabbitClient, qName)
          } yield consumer

        val identifier         = UUID.randomUUID().toString
        val computedOperations = 4
        val account = Account(
          identifier,
          CoinFamily.Bitcoin,
          Coin.Btc,
          AccountGroup("group")
        )
        val operationsComputedNotification =
          OperationsComputedNotification(
            account,
            UUID.randomUUID(),
            computedOperations
          )

        val routingKey = RabbitNotificationService.routingKey(operationsComputedNotification)

        for {
          publish              <- notificationPublisher
          notifications        <- routedNotifications(routingKey)(channel)
          _                    <- publish(operationsComputedNotification)
          receivedNotification <- consumeNotification[OperationsComputedNotification](notifications)
        } yield {
          it should "contain the pushed notification" in {
            receivedNotification.account.id shouldBe account.id
            receivedNotification.operationsCount shouldBe computedOperations
          }
        }
      }
  }
}
