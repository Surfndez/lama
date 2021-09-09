package co.ledger.lama.scheduler.domain.models

import co.ledger.lama.scheduler.domain.models.implicits._
import co.ledger.lama.scheduler.protobuf
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder, JsonObject}

case class AccountInfo(
    account: Account,
    syncFrequency: Long,
    lastSyncEvent: Option[SyncEvent[JsonObject]],
    label: Option[String]
) {
  def toProto: protobuf.AccountInfoResult =
    protobuf.AccountInfoResult(
      Some(account.toProto),
      syncFrequency,
      lastSyncEvent.map(_.toProto),
      label.map(protobuf.AccountLabel(_))
    )
}

object AccountInfo {
  implicit val decoder: Decoder[AccountInfo] =
    deriveConfiguredDecoder[AccountInfo]
  implicit val encoder: Encoder[AccountInfo] =
    deriveConfiguredEncoder[AccountInfo]

  // FIXME: Should fromProto be failible ?
  def fromProto(proto: protobuf.AccountInfoResult): AccountInfo =
    AccountInfo(
      Account.fromProto(proto.account.get),
      proto.syncFrequency,
      proto.lastSyncEvent.map(SyncEvent.fromProto[JsonObject]),
      proto.label.map(_.value)
    )
}
