package co.ledger.lama.bitcoin.transactor.services

import co.ledger.lama.bitcoin.common.models.Scheme
import co.ledger.lama.common.models.{BitcoinLikeCoin, Coin}

object Fees {

  private def estimator(coin: BitcoinLikeCoin): FeesEstimator = coin match {
    case Coin.Btc        => btcEstimator
    case Coin.BtcTestnet => btcEstimator
    case Coin.BtcRegtest => btcEstimator
  }

  def estimateTxFees(coin: BitcoinLikeCoin)(outputSize: Int): Int =
    estimator(coin).txFees(outputSize)

  def estimateSingleUtxoFees(coin: BitcoinLikeCoin)(scheme: Scheme): Int =
    estimator(coin).singleUtxoFees(scheme)

  trait FeesEstimator {
    def txFees(outputSize: Int): Int
    def singleUtxoFees(scheme: Scheme): Int
  }

  private val btcEstimator: FeesEstimator = new FeesEstimator {

    private val txSkeletonFees: Int = 10

    private def varLengthIntegerSize(outputSize: Long): Int =
      outputSize match {
        case s if s <= 253L        => 1
        case s if s <= 65535L      => 3
        case s if s <= 4294967295L => 5
        case _                     => 9
      }

    def txFees(outputSize: Int): Int =
      varLengthIntegerSize(outputSize) + (34 * outputSize) + txSkeletonFees

    def singleUtxoFees(scheme: Scheme): Int =
      scheme match {
        case Scheme.Bip44 => 148
        case Scheme.Bip49 => 90
        case Scheme.Bip84 => 68
        case _            => 148
      }
  }

}
