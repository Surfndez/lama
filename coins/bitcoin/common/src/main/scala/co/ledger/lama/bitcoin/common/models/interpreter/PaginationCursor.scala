package co.ledger.lama.bitcoin.common.models.interpreter

import co.ledger.lama.common.models.implicits._
import co.ledger.lama.bitcoin.interpreter.protobuf
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}

case class PaginationCursor(
    previous: Option[String],
    next: Option[String]
) {
  def toProto: protobuf.PaginationCursor =
    protobuf.PaginationCursor(
      previous = previous.getOrElse(""),
      next = next.getOrElse("")
    )
}

object PaginationCursor {
  implicit val encoder: Encoder[PaginationCursor] =
    deriveConfiguredEncoder[PaginationCursor]

  implicit val decoder: Decoder[PaginationCursor] =
    deriveConfiguredDecoder[PaginationCursor]

  def fromProto(proto: protobuf.PaginationCursor): PaginationCursor =
    PaginationCursor(
      previous = if (proto.previous.nonEmpty) Some(proto.previous) else None,
      next = if (proto.next.nonEmpty) Some(proto.next) else None
    )
}
