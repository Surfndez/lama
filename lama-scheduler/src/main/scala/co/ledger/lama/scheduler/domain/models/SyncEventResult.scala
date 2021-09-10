package co.ledger.lama.scheduler.domain.models

import co.ledger.lama.common.utils.UuidUtils
import co.ledger.lama.scheduler.protobuf
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import co.ledger.lama.scheduler.domain.models.implicits._
import io.circe.{Decoder, Encoder}

import java.util.UUID

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
