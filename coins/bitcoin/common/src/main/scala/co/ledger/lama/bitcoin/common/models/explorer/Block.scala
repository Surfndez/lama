package co.ledger.lama.bitcoin.common.models.explorer

import java.time.Instant

import co.ledger.lama.bitcoin.interpreter.protobuf
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.common.utils.TimestampProtoUtils
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

case class Block(
    hash: String,
    height: Long,
    time: Instant
) {
  def toProto: protobuf.Block =
    protobuf.Block(
      hash,
      height,
      Some(TimestampProtoUtils.serialize(time))
    )
}

object Block {
  implicit val encoder: Encoder[Block] = deriveConfiguredEncoder[Block]
  implicit val decoder: Decoder[Block] = deriveConfiguredDecoder[Block].map(b =>
    if (b.hash.startsWith("0x")) b.copy(hash = b.hash.substring(2)) else b
  )

  def fromProto(proto: protobuf.Block): Block =
    Block(
      proto.hash,
      proto.height,
      proto.time.map(TimestampProtoUtils.deserialize).getOrElse(Instant.now)
    )

  implicit val ordering: Ordering[Block] = Ordering.by(b => (b.height, b.time))
}
