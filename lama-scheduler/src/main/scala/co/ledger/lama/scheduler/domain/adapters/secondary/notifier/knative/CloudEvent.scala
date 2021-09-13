package co.ledger.lama.scheduler.domain.adapters.secondary.notifier.knative

import co.ledger.lama.scheduler.domain.models.WorkableEvent
import io.circe.{Encoder, JsonObject}
import co.ledger.lama.scheduler.domain.models.implicits._
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder

import java.util.UUID

case class CloudEvent[A](specversion: String, `type`: String, source: String, id: UUID, data: A)

object CloudEvent {
  implicit def cloudEventEncoder[A: Encoder]: Encoder[CloudEvent[A]] =
    deriveConfiguredEncoder[CloudEvent[A]]

  def fromWorkableEvent(id: UUID, e: WorkableEvent[JsonObject]): CloudEvent[SynchronizationInfo] =
    CloudEvent(
      specversion = "1.0",
      `type` = "TBD",
      source = "TBD",
      id = id,
      data = SynchronizationInfo(
        correlationId = e.syncId,
        //TODO change me to wd style
        accountUid = e.account.id.toString,
        coin = e.account.coin.name,
        rawData = e.cursor.getOrElse(JsonObject.empty),
        reseedFromBlockHash = None
      )
    )
}
