package co.ledger.lama.common.services

import cats.data.Kleisli
import cats.effect.IO
import cats.implicits.catsSyntaxApplicativeId
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.models.Notification
import dev.profunktor.fs2rabbit.effects.MessageEncoder
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeType.Topic
import dev.profunktor.fs2rabbit.model._
import io.circe.syntax._
import java.nio.charset.StandardCharsets

object RabbitNotificationService extends ContextLogging {

  type NotificationPublisher = Notification => IO[Unit]

  implicit val me: MessageEncoder[IO, Notification] =
    Kleisli[IO, Notification, AmqpMessage[Array[Byte]]] { s =>
      AmqpMessage(
        payload = s.asJson.noSpaces.getBytes(StandardCharsets.UTF_8),
        properties = AmqpProperties.empty
      ).pure[IO]
    }

  def publisher(
      exchangeName: ExchangeName,
      routingKey: Notification => RoutingKey
  )(implicit
      rabbitClient: RabbitClient[IO],
      channel: AMQPChannel
  ): IO[NotificationPublisher] =
    for {
      _ <- rabbitClient.declareExchange(exchangeName, Topic)
      p <- rabbitClient.createRoutingPublisher(exchangeName)
    } yield { n: Notification =>
      implicit val lc: LamaLogContext =
        LamaLogContext().withAccount(n.account).withFollowUpId(n.syncId)
      p(routingKey(n))(n) *> log.info(s"Published notification $n")
    }

  def routingKey(notification: Notification): RoutingKey =
    RoutingKey(
      s"${notification.account.group.name}.${notification.account.coinFamily.name}.${notification.account.coin.name}.${notification.account.id.toString}"
    )

}
