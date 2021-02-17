package co.ledger.lama.bitcoin.transactor.services

import co.ledger.lama.bitcoin.common.models.Scheme
import co.ledger.lama.common.models.Coin
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class FeesSpec extends AnyFlatSpecLike with Matchers {

  "A prepared transaction with outputs" should "have fees calculated from it's size." in {

    // 1 byte for for var int
    // 34 * 1 output
    // 10 for tx skeleton
    Fees.estimateTxFees(Coin.Btc)(1) should be(1 + 34 + 10)

    // 3 byte for for var int ( > 253)
    // 34 * 300 output
    // 10 for tx skeleton
    Fees.estimateTxFees(Coin.Btc)(300) should be(3 + 34 * 300 + 10)

  }

  "A single UXTO" should "have fees calculated from it's account Scheme" in {
    Fees.estimateSingleUtxoFees(Coin.Btc)(Scheme.Bip49) should be(90)
  }

}
