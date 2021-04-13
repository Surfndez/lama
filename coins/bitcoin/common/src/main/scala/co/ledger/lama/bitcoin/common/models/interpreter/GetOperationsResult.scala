package co.ledger.lama.bitcoin.common.models.interpreter

import co.ledger.lama.bitcoin.interpreter.protobuf
import co.ledger.lama.common.models.implicits._
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

case class GetOperationsResult(
    operations: List[Operation],
    total: Int,
    cursor: Option[PaginationCursor]
) {
  def toProto: protobuf.GetOperationsResult =
    protobuf.GetOperationsResult(
      operations.map(_.toProto),
      total,
      cursor.map(_.toProto)
    )
}

object GetOperationsResult {

  implicit val decoder: Decoder[GetOperationsResult] = deriveConfiguredDecoder[GetOperationsResult]
  implicit val encoder: Encoder[GetOperationsResult] = deriveConfiguredEncoder[GetOperationsResult]

  def fromProto(proto: protobuf.GetOperationsResult): GetOperationsResult =
    GetOperationsResult(
      proto.operations.map(Operation.fromProto).toList,
      proto.total,
      proto.cursor.map(PaginationCursor.fromProto)
    )
}
