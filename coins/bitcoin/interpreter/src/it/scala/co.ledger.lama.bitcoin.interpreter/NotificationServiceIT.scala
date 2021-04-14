package co.ledger.lama.bitcoin.interpreter

import java.util.UUID
import cats.effect.{ContextShift, IO, Resource, Timer}
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

import scala.concurrent.ExecutionContext

class NotificationServiceIT extends AnyFlatSpecLike with Matchers {

  def consumeNotification[T <: Notification](consumer: Stream[IO, T]): IO[T] =
    consumer.take(1).compile.last.map(_.get)

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val t: Timer[IO]         = IO.timer(ExecutionContext.global)

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

        val accountId: UUID    = UUID.randomUUID()
        val computedOperations = 4
        val operationsComputedNotification =
          OperationsComputedNotification(
            accountId,
            CoinFamily.Bitcoin,
            Coin.Btc,
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
            receivedNotification.accountId shouldBe accountId
            receivedNotification.operationsCount shouldBe computedOperations
          }
        }
      }
  }
}
