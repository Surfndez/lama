package co.ledger.lama.bitcoin.common.models.interpreter

import co.ledger.lama.bitcoin.interpreter.protobuf

case class GetUtxosResult(
    utxos: List[ConfirmedUtxo],
    total: Int,
    truncated: Boolean
) {
  def toProto: protobuf.GetUtxosResult =
    protobuf.GetUtxosResult(
      utxos.map(_.toProto),
      total,
      truncated
    )
}

object GetUtxosResult {
  def fromProto(proto: protobuf.GetUtxosResult): GetUtxosResult =
    GetUtxosResult(
      proto.utxos.map(ConfirmedUtxo.fromProto).toList,
      proto.total,
      proto.truncated
    )
}
