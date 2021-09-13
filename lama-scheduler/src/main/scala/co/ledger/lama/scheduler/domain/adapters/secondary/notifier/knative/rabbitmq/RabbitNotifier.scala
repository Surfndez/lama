package co.ledger.lama.scheduler.domain.adapters.secondary.notifier.knative.rabbitmq

import cats.effect.IO
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import co.ledger.lama.scheduler.domain.adapters.secondary.notifier.knative.{CloudEvent, SynchronizationInfo}
import co.ledger.lama.scheduler.domain.models.WorkableEvent
import co.ledger.lama.scheduler.domain.services.Notifier
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{ExchangeName, RoutingKey}
import fs2.Stream
import io.circe.JsonObject
import io.circe.syntax._

import java.util.UUID

final class RabbitNotifier(rabbit: RabbitClient[IO], exchangeName: ExchangeName, routingKey: RoutingKey) extends Notifier with ContextLogging {

  def publish(event: WorkableEvent[JsonObject]): IO[Unit] =
    publisher
      .evalMap(p =>
        IO(UUID.randomUUID()).flatMap(id => p(CloudEvent.fromWorkableEvent(id, event))) *>
          log.info(s"Published event to worker: ${event.asJson.toString}")(LamaLogContext().withAccount(event.account).withFollowUpId(event.syncId)
        )
      )
      .compile
      .drain

  private val publisher: Stream[IO, CloudEvent[SynchronizationInfo] => IO[Unit]] =
    RabbitUtils.createPublisher[CloudEvent[SynchronizationInfo]](
      rabbit,
      exchangeName,
      routingKey
    )
}
