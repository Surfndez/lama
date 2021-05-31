package co.ledger.lama.bitcoin.api.models

import java.time.Instant

import cats.data.NonEmptyList
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.bitcoin.common.models.interpreter
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

case class ConfirmedUtxo(
    height: Long,
    confirmations: Long,
    transactionHash: String,
    outputIndex: Int,
    value: BigInt,
    address: String,
    scriptHex: String,
    changeType: Option[interpreter.ChangeType],
    derivation: NonEmptyList[Int],
    time: Instant
) {
  def toCommon: interpreter.Utxo =
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

object ConfirmedUtxo {
  implicit val encoder: Encoder[ConfirmedUtxo] = deriveConfiguredEncoder[ConfirmedUtxo]
  implicit val decoder: Decoder[ConfirmedUtxo] = deriveConfiguredDecoder[ConfirmedUtxo]

  def fromCommon(utxo: interpreter.ConfirmedUtxo): ConfirmedUtxo = {
    ConfirmedUtxo(
      utxo.height,
      utxo.confirmations,
      utxo.transactionHash,
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
