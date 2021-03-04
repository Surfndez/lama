package co.ledger.lama.bitcoin.api

import co.ledger.lama.common.models.implicits._
import co.ledger.lama.bitcoin.api.models.accountManager.CreationRequest
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto._

case class TestAccount(
    registerRequest: CreationRequest,
    expected: AccountExpectedResult
)

object TestAccount {
  implicit val decoder: Decoder[TestAccount] = deriveConfiguredDecoder[TestAccount]
  implicit val encoder: Encoder[TestAccount] = deriveConfiguredEncoder[TestAccount]
}

case class AccountExpectedResult(
    extendedPublicKey: String,
    opsSize: Int,
    utxosSize: Int,
    lastTxHash: String,
    balance: Long,
    balanceHistorySize: Int,
    amountReceived: Long,
    amountSent: Long
)

object AccountExpectedResult {
  implicit val decoder: Decoder[AccountExpectedResult] =
    deriveConfiguredDecoder[AccountExpectedResult]
  implicit val encoder: Encoder[AccountExpectedResult] =
    deriveConfiguredEncoder[AccountExpectedResult]
}
