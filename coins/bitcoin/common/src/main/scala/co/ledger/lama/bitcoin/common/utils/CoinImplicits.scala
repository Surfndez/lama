package co.ledger.lama.bitcoin.common.utils

import co.ledger.lama.bitcoin.common.models.{BitcoinLikeNetwork, BitcoinNetwork, LitecoinNetwork}
import co.ledger.lama.common.models.Coin

object CoinImplicits {

  implicit class CoinBitcoinUtils(coin: Coin) {

    def toNetwork: BitcoinLikeNetwork = {
      coin match {
        case Coin.Btc        => BitcoinNetwork.MainNet
        case Coin.BtcTestnet => BitcoinNetwork.TestNet3
        case Coin.BtcRegtest => BitcoinNetwork.RegTest
        case Coin.Ltc        => LitecoinNetwork.MainNet
        case _               => BitcoinNetwork.Unspecified
      }
    }

  }

}
