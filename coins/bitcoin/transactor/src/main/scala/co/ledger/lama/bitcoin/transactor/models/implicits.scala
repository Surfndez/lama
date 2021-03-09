package co.ledger.lama.bitcoin.transactor.models

import co.ledger.lama.bitcoin.common.models.{BitcoinLikeNetwork, BitcoinNetwork, LitecoinNetwork}
import co.ledger.protobuf.bitcoin.libgrpc

object implicits {

  implicit class BitcoinLikeNetworkLibGrpcProtoImplicit(network: BitcoinLikeNetwork) {

    def toLibGrpcProto: libgrpc.ChainParams =
      network match {
        case BitcoinNetwork.MainNet =>
          libgrpc.ChainParams.defaultInstance.withBitcoinNetwork(
            libgrpc.BitcoinNetwork.BITCOIN_NETWORK_MAINNET
          )

        case BitcoinNetwork.TestNet3 =>
          libgrpc.ChainParams.defaultInstance.withBitcoinNetwork(
            libgrpc.BitcoinNetwork.BITCOIN_NETWORK_TESTNET3
          )

        case BitcoinNetwork.RegTest =>
          libgrpc.ChainParams.defaultInstance.withBitcoinNetwork(
            libgrpc.BitcoinNetwork.BITCOIN_NETWORK_REGTEST
          )

        case LitecoinNetwork.MainNet =>
          libgrpc.ChainParams.defaultInstance.withLitecoinNetwork(
            libgrpc.LitecoinNetwork.LITECOIN_NETWORK_MAINNET
          )

        case _ =>
          libgrpc.ChainParams.defaultInstance.withBitcoinNetwork(
            libgrpc.BitcoinNetwork.BITCOIN_NETWORK_UNSPECIFIED
          )
      }

  }

}
