package co.ledger.lama.common.models

import java.util.UUID

import co.ledger.lama.common.utils.UuidUtils
import co.ledger.lama.scheduler.protobuf
import co.ledger.lama.common.models.implicits._
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}

case class SyncEventResult(accountId: UUID, syncId: UUID) {
  def toProto: protobuf.SyncEventResult =
    protobuf.SyncEventResult(
      UuidUtils.uuidToBytes(accountId),
      UuidUtils.uuidToBytes(syncId)
    )
}

object SyncEventResult {
  implicit val decoder: Decoder[SyncEventResult] =
    deriveConfiguredDecoder[SyncEventResult]
  implicit val encoder: Encoder[SyncEventResult] =
    deriveConfiguredEncoder[SyncEventResult]

  def fromProto(proto: protobuf.SyncEventResult): SyncEventResult =
    SyncEventResult(
      accountId = UuidUtils.bytesToUuid(proto.accountId).get,
      syncId = UuidUtils.bytesToUuid(proto.syncId).get
    )
}
