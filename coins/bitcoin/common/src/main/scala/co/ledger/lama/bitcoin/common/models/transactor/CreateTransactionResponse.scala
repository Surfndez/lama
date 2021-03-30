package co.ledger.lama.bitcoin.common.models.transactor

import co.ledger.lama.bitcoin.common.models.interpreter.{SpendableTxo, Utxo}
import co.ledger.lama.bitcoin.transactor.protobuf

case class CreateTransactionResponse(
    hex: String,
    hash: String,
    witnessHash: String,
    utxos: List[SpendableTxo],
    outputs: List[PrepareTxOutput],
    fee: Long,
    feePerKb: Long
) {
  def toProto: protobuf.CreateTransactionResponse =
    protobuf.CreateTransactionResponse(
      hex,
      hash,
      witnessHash,
      utxos.map(_.toCommon.toProto),
      outputs.map(_.toProto),
      fee,
      feePerKb
    )
}

object CreateTransactionResponse {
  def fromProto(proto: protobuf.CreateTransactionResponse): CreateTransactionResponse =
    CreateTransactionResponse(
      proto.hex,
      proto.hash,
      proto.witnessHash,
      proto.utxos
        .map(protoUtxo => SpendableTxo.fromCommon(Utxo.fromProto(protoUtxo), "UNKNOWN"))
        .toList,
      proto.outputs.map(PrepareTxOutput.fromProto).toList,
      proto.fee,
      proto.feePerKb
    )
}
