package co.ledger.lama.scheduler.domain.adapters.secondary.notifier.knative.http

import cats.effect.{ContextShift, IO, Resource}
import co.ledger.lama.common.services.Clients
import co.ledger.lama.scheduler.domain.adapters.secondary.notifier.knative.CloudEvent
import co.ledger.lama.scheduler.domain.models.WorkableEvent
import co.ledger.lama.scheduler.domain.services.Notifier
import io.circe.JsonObject
import io.circe.generic.auto._
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.Client

import java.util.UUID

final class HttpNotifier private (httpClient: Client[IO], uri: Uri) extends Notifier {
  override def publish(e: WorkableEvent[JsonObject]): IO[Unit] =
    uuid.flatMap { id =>
      val req: Request[IO] =
        Request(
          method = Method.POST,
          uri = uri,
          headers = Headers.of(Header("Content-Type", "application/cloudevents+json"))
        )
          .withEntity(CloudEvent.fromWorkableEvent(id, e))

      httpClient
        .expect[Unit](req)
        .void
    }

  private val uuid = IO(UUID.randomUUID())
}

object HttpNotifier {
  def apply(config: HttpNotifierConfig)(implicit cs: ContextShift[IO]): Resource[IO, HttpNotifier] =
    Clients.htt4s.map(c => new HttpNotifier(c, config.uri))
}
