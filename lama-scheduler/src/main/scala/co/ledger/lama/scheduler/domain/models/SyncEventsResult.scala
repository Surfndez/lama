package co.ledger.lama.scheduler.domain.models

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import co.ledger.lama.scheduler.domain.models.implicits._

case class SyncEventsResult[T](syncEvents: List[SyncEvent[T]], total: Int)

object SyncEventsResult {
  implicit def decoder[T: Decoder]: Decoder[SyncEventsResult[T]] =
    deriveConfiguredDecoder[SyncEventsResult[T]]
  implicit def encoder[T: Encoder]: Encoder[SyncEventsResult[T]] =
    deriveConfiguredEncoder[SyncEventsResult[T]]
}
