package co.ledger.lama.bitcoin.common.models.interpreter

import java.time.Instant

import cats.data.NonEmptyList
import co.ledger.lama.bitcoin.common.models.interpreter

case class SpendableTxo(
    transactionHash: String,
    transactionRawHex: String,
    outputIndex: Int,
    value: BigInt,
    address: String,
    scriptHex: String,
    changeType: Option[interpreter.ChangeType],
    derivation: NonEmptyList[Int],
    time: Instant
) {
  def toCommon: Utxo =
    interpreter.Utxo(
      transactionHash,
      outputIndex,
      value,
      address,
      scriptHex,
      changeType,
      derivation,
      time
    )
}

object SpendableTxo {
  def fromCommon(utxo: interpreter.Utxo, rawHex: String): SpendableTxo =  {
    SpendableTxo(
      utxo.transactionHash,
      rawHex,
      utxo.outputIndex,
      utxo.value,
      utxo.address,
      utxo.scriptHex,
      utxo.changeType,
      utxo.derivation,
      utxo.time
    )
  }
}
