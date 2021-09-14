package co.ledger.lama.scheduler.domain.models

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import co.ledger.lama.scheduler.domain.models.implicits._
import io.circe.{Decoder, Encoder}

import java.util.UUID

case class SyncEventResult(accountId: UUID, syncId: UUID)

object SyncEventResult {
  implicit val decoder: Decoder[SyncEventResult] =
    deriveConfiguredDecoder[SyncEventResult]
  implicit val encoder: Encoder[SyncEventResult] =
    deriveConfiguredEncoder[SyncEventResult]
}
