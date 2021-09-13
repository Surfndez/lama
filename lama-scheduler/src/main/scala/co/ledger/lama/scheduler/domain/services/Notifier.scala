package co.ledger.lama.scheduler.domain.services

import cats.effect.IO
import co.ledger.lama.scheduler.domain.models.WorkableEvent
import io.circe.JsonObject

trait Notifier {
  def publish(e: WorkableEvent[JsonObject]): IO[Unit]
}
