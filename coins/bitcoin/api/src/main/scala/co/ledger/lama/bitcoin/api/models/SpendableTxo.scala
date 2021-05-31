package co.ledger.lama.bitcoin.api.models

import java.time.Instant

import cats.data.NonEmptyList
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.bitcoin.common.models.interpreter
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

case class SpendableTxo(
    transactionHash: String,
    transactionRawHex: String,
    outputIndex: Int,
    value: String,
    address: String,
    scriptHex: String,
    changeType: Option[interpreter.ChangeType],
    derivation: NonEmptyList[Int],
    publicKey: String,
    time: Instant
) {
  def toCommon: interpreter.Utxo =
    interpreter.Utxo(
      transactionHash,
      outputIndex,
      BigInt(value),
      address,
      scriptHex,
      changeType,
      derivation,
      time
    )
}

object SpendableTxo {
  implicit val encoder: Encoder[SpendableTxo] = deriveConfiguredEncoder[SpendableTxo]
  implicit val decoder: Decoder[SpendableTxo] = deriveConfiguredDecoder[SpendableTxo]

  def fromCommon(utxo: interpreter.SpendableTxo, pubKey: String): SpendableTxo = {
    SpendableTxo(
      utxo.transactionHash,
      utxo.transactionRawHex,
      utxo.outputIndex,
      utxo.value.toString,
      utxo.address,
      utxo.scriptHex,
      utxo.changeType,
      utxo.derivation,
      pubKey,
      utxo.time
    )
  }
}
