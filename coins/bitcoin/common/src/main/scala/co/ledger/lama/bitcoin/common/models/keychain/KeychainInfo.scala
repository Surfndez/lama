package co.ledger.lama.bitcoin.common.models.keychain

import java.util.UUID
import co.ledger.lama.bitcoin.common.models.{BitcoinLikeNetwork, BitcoinNetwork, Scheme}
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.common.utils.UuidUtils
import co.ledger.protobuf.bitcoin.keychain
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

case class KeychainInfo(
    keychainId: UUID,
    externalDescriptor: String,
    internalDescriptor: String,
    extendedPublicKey: String,
    slip32ExtendedPublicKey: String,
    lookaheadSize: Int,
    scheme: Scheme,
    network: BitcoinLikeNetwork
) {
  def toProto: keychain.KeychainInfo =
    keychain.KeychainInfo(
      UuidUtils.uuidToBytes(keychainId),
      externalDescriptor,
      internalDescriptor,
      extendedPublicKey,
      slip32ExtendedPublicKey,
      lookaheadSize,
      scheme.toProto,
      Some(network.toKeychainChainParamsProto)
    )
}

object KeychainInfo {

  implicit val encoder: Encoder[KeychainInfo] = deriveConfiguredEncoder[KeychainInfo]
  implicit val decoder: Decoder[KeychainInfo] = deriveConfiguredDecoder[KeychainInfo]

  def fromProto(proto: keychain.KeychainInfo): KeychainInfo =
    KeychainInfo(
      UuidUtils.unsafeBytesToUuid(proto.keychainId),
      proto.externalDescriptor,
      proto.internalDescriptor,
      proto.extendedPublicKey,
      proto.slip32ExtendedPublicKey,
      proto.lookaheadSize,
      Scheme.fromProto(proto.scheme),
      proto.chainParams.map(BitcoinLikeNetwork.fromProto).getOrElse(BitcoinNetwork.Unspecified)
    )
}
