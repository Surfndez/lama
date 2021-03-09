package co.ledger.lama.bitcoin.common.models

import io.circe.{Decoder, Encoder}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import co.ledger.protobuf.bitcoin.keychain

sealed trait BitcoinLikeNetwork {
  val name: String
  def toKeychainChainParamsProto: keychain.ChainParams
}

object BitcoinLikeNetwork {
  val all: Map[String, BitcoinLikeNetwork] =
    Map(
      BitcoinNetwork.MainNet.name     -> BitcoinNetwork.MainNet,
      BitcoinNetwork.TestNet3.name    -> BitcoinNetwork.TestNet3,
      BitcoinNetwork.RegTest.name     -> BitcoinNetwork.RegTest,
      BitcoinNetwork.Unspecified.name -> BitcoinNetwork.Unspecified,
      LitecoinNetwork.MainNet.name    -> LitecoinNetwork.MainNet
    )

  def fromKey(key: String): Option[BitcoinLikeNetwork] = all.get(key)

  implicit val encoder: Encoder[BitcoinLikeNetwork] =
    Encoder.encodeString.contramap(_.name)

  implicit val decoder: Decoder[BitcoinLikeNetwork] =
    Decoder.decodeString.emap(fromKey(_).toRight("Could not decode as BitcoinLikeNetwork"))

  implicit val configReader: ConfigReader[BitcoinLikeNetwork] =
    ConfigReader.fromString(str =>
      fromKey(str).toRight(CannotConvert(str, "BitcoinLikeNetwork", "unknown"))
    )

  def fromProto(proto: keychain.ChainParams): BitcoinLikeNetwork =
    proto.network match {
      case keychain.ChainParams.Network
            .BitcoinNetwork(keychain.BitcoinNetwork.BITCOIN_NETWORK_MAINNET) =>
        BitcoinNetwork.MainNet

      case keychain.ChainParams.Network
            .BitcoinNetwork(keychain.BitcoinNetwork.BITCOIN_NETWORK_TESTNET3) =>
        BitcoinNetwork.TestNet3

      case keychain.ChainParams.Network
            .BitcoinNetwork(keychain.BitcoinNetwork.BITCOIN_NETWORK_REGTEST) =>
        BitcoinNetwork.RegTest

      case keychain.ChainParams.Network
            .LitecoinNetwork(keychain.LitecoinNetwork.LITECOIN_NETWORK_MAINNET) =>
        LitecoinNetwork.MainNet

      case _ => BitcoinNetwork.Unspecified
    }
}

sealed trait BitcoinNetwork extends BitcoinLikeNetwork {
  def toKeychainNetworkProto: keychain.BitcoinNetwork

  def toKeychainChainParamsProto: keychain.ChainParams =
    keychain.ChainParams.defaultInstance.withBitcoinNetwork(toKeychainNetworkProto)
}

object BitcoinNetwork {

  final case object MainNet extends BitcoinNetwork {
    val name: String = "mainnet"

    def toKeychainNetworkProto: keychain.BitcoinNetwork =
      keychain.BitcoinNetwork.BITCOIN_NETWORK_MAINNET
  }

  final case object TestNet3 extends BitcoinNetwork {
    val name: String = "testnet"

    def toKeychainNetworkProto: keychain.BitcoinNetwork =
      keychain.BitcoinNetwork.BITCOIN_NETWORK_TESTNET3
  }

  final case object RegTest extends BitcoinNetwork {
    val name: String = "regtest"

    def toKeychainNetworkProto: keychain.BitcoinNetwork =
      keychain.BitcoinNetwork.BITCOIN_NETWORK_REGTEST
  }

  final case object Unspecified extends BitcoinNetwork {
    val name: String = "unspecified"

    def toKeychainNetworkProto: keychain.BitcoinNetwork =
      keychain.BitcoinNetwork.BITCOIN_NETWORK_UNSPECIFIED
  }

}

object LitecoinNetwork {

  final case object MainNet extends BitcoinLikeNetwork {
    val name: String = "litecoin_mainnet"

    def toKeychainChainParamsProto: keychain.ChainParams =
      keychain.ChainParams.defaultInstance
        .withLitecoinNetwork(keychain.LitecoinNetwork.LITECOIN_NETWORK_MAINNET)
  }

}
