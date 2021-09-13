package co.ledger.lama.scheduler.domain.adapters.secondary.notifier.knative

import io.circe.generic.extras.semiauto.deriveConfiguredEncoder
import io.circe.{Encoder, JsonObject}
import co.ledger.lama.scheduler.domain.models.implicits._

import java.util.UUID

case class SynchronizationInfo(
    correlationId: UUID,
    accountUid: String,
    coin: String,
    rawData: JsonObject,
    reseedFromBlockHash: Option[String]
)

object SynchronizationInfo {

  implicit val synchronizationInfoEncoder: Encoder[SynchronizationInfo] =
    deriveConfiguredEncoder[SynchronizationInfo]
}