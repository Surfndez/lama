package co.ledger.lama.scheduler.domain.models

import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import co.ledger.lama.scheduler.domain.models.implicits._

case class AccountsResult(accounts: List[AccountInfo], total: Int) {}

object AccountsResult {

  implicit val decoder: Decoder[AccountsResult] =
    deriveConfiguredDecoder[AccountsResult]
  implicit val encoder: Encoder[AccountsResult] =
    deriveConfiguredEncoder[AccountsResult]
}
