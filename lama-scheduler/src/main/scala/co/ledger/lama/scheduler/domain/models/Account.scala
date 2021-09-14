package co.ledger.lama.scheduler.domain.models

import co.ledger.lama.scheduler.domain.models.implicits._

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto._

case class Account(
    identifier: String,
    coinFamily: CoinFamily,
    coin: Coin,
    group: AccountGroup
) {
  lazy val id: UUID = UUID.nameUUIDFromBytes((identifier + coinFamily + coin + group).getBytes)
}

object Account {
  implicit val encoder: Encoder[Account] = deriveConfiguredEncoder[Account]
  implicit val decoder: Decoder[Account] = deriveConfiguredDecoder[Account]
}
