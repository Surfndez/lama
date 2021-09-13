package co.ledger.lama.scheduler.domain.adapters.secondary.notifier.lama

import cats.effect.IO
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import co.ledger.lama.scheduler.domain.models.WorkableEvent
import co.ledger.lama.scheduler.domain.services.Notifier
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{ExchangeName, RoutingKey}
import fs2.Stream
import io.circe.JsonObject
import io.circe.syntax._


final class RabbitNotifier(rabbit: RabbitClient[IO], exchangeName: ExchangeName, routingKey: RoutingKey) extends Notifier with ContextLogging {

  def publish(event: WorkableEvent[JsonObject]): IO[Unit] =
    publisher
      .evalMap(p =>
        p(event) *> log.info(s"Published event to worker: ${event.asJson.toString}")(
          LamaLogContext().withAccount(event.account).withFollowUpId(event.syncId)
        )
      )
      .compile
      .drain

  private val publisher: Stream[IO, WorkableEvent[JsonObject] => IO[Unit]] =
    RabbitUtils.createPublisher[WorkableEvent[JsonObject]](
      rabbit,
      exchangeName,
      routingKey
    )
}
