package co.ledger.lama.bitcoin.api.models

import co.ledger.lama.bitcoin.common.models.transactor.{
  CoinSelectionStrategy,
  FeeLevel,
  PrepareTxOutput,
  RawTransaction
}
import co.ledger.lama.common.models.implicits._
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto._

object transactor {

  case class CreateTransactionRequest(
      coinSelection: CoinSelectionStrategy,
      outputs: List[PrepareTxOutput],
      feeLevel: FeeLevel,
      customFee: Option[Long],
      maxUtxos: Option[Int]
  )

  object CreateTransactionRequest {
    implicit val encoder: Encoder[CreateTransactionRequest] =
      deriveConfiguredEncoder[CreateTransactionRequest]
    implicit val decoder: Decoder[CreateTransactionRequest] =
      deriveConfiguredDecoder[CreateTransactionRequest]
  }

  case class BroadcastTransactionRequest(
      rawTransaction: RawTransaction,
      signatures: List[String]
  )

  object BroadcastTransactionRequest {
    implicit val encoder: Encoder[BroadcastTransactionRequest] =
      deriveConfiguredEncoder[BroadcastTransactionRequest]
    implicit val decoder: Decoder[BroadcastTransactionRequest] =
      deriveConfiguredDecoder[BroadcastTransactionRequest]
  }

  case class GenerateSignaturesRequest(
      rawTransaction: RawTransaction,
      privKey: String
  )

  object GenerateSignaturesRequest {
    implicit val encoder: Encoder[GenerateSignaturesRequest] =
      deriveConfiguredEncoder[GenerateSignaturesRequest]
    implicit val decoder: Decoder[GenerateSignaturesRequest] =
      deriveConfiguredDecoder[GenerateSignaturesRequest]
  }

}
