package co.ledger.lama.bitcoin.common.models.transactor

import co.ledger.lama.bitcoin.transactor.protobuf

case class PrepareTxOutput(
    address: String,
    value: BigInt,
    change: Option[List[Int]] = None
) {
  def toProto: protobuf.PrepareTxOutput =
    protobuf.PrepareTxOutput(
      address,
      value.toString,
      change.map(protobuf.Derivation(_))
    )
}

object PrepareTxOutput {
  def fromProto(proto: protobuf.PrepareTxOutput): PrepareTxOutput =
    PrepareTxOutput(
      proto.address,
      BigInt(proto.value),
      proto.change.map(_.path.toList)
    )
}
