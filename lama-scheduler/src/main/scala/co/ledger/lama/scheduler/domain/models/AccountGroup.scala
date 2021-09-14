package co.ledger.lama.scheduler.domain.models

import co.ledger.lama.scheduler.domain.models.implicits._
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}

case class AccountGroup(name: String) extends AnyVal {}

object AccountGroup {
  implicit val decoder: Decoder[AccountGroup] =
    deriveConfiguredDecoder[AccountGroup]
  implicit val encoder: Encoder[AccountGroup] =
    deriveConfiguredEncoder[AccountGroup]
}
