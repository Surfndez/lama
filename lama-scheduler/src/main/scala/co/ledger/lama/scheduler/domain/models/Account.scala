package co.ledger.lama.scheduler.domain.models

import co.ledger.lama.scheduler.domain.models.implicits._
import co.ledger.lama.scheduler.protobuf

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

  def toProto: protobuf.Account = protobuf.Account(
    identifier,
    coinFamily.toProto,
    coin.toProto,
    Some(group.toProto)
  )
}

object Account {
  implicit val encoder: Encoder[Account] = deriveConfiguredEncoder[Account]
  implicit val decoder: Decoder[Account] = deriveConfiguredDecoder[Account]

  def fromProto(proto: protobuf.Account): Account =
    Account(
      proto.identifier,
      CoinFamily.fromProto(proto.coinFamily),
      Coin.fromProto(proto.coin),
      AccountGroup.fromProto(proto.getGroup)
    )
}
