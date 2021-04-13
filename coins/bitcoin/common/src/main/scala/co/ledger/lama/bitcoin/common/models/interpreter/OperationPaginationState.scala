package co.ledger.lama.bitcoin.common.models.interpreter

import co.ledger.lama.common.models.implicits._
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

final case class OperationPaginationState(uid: Operation.UID, blockHeight: Long)

object OperationPaginationState {
  implicit val encoder: Encoder[OperationPaginationState] =
    deriveConfiguredEncoder[OperationPaginationState]

  implicit val decoder: Decoder[OperationPaginationState] =
    deriveConfiguredDecoder[OperationPaginationState]
}
