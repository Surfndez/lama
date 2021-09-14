package co.ledger.lama.scheduler.domain.models

import co.ledger.lama.scheduler.domain.models.implicits._
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder, JsonObject}

case class AccountInfo(
    account: Account,
    syncFrequency: Long,
    lastSyncEvent: Option[SyncEvent[JsonObject]],
    label: Option[String]
)

object AccountInfo {
  implicit val decoder: Decoder[AccountInfo] =
    deriveConfiguredDecoder[AccountInfo]
  implicit val encoder: Encoder[AccountInfo] =
    deriveConfiguredEncoder[AccountInfo]
}
