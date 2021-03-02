package co.ledger.lama.bitcoin.common.clients.grpc

import java.util.UUID
import cats.effect.{ContextShift, IO}
import co.ledger.lama.bitcoin.common.models.transactor.{
  BroadcastTransaction,
  CoinSelectionStrategy,
  FeeLevel,
  PrepareTxOutput,
  RawTransaction
}
import co.ledger.lama.bitcoin.transactor.protobuf
import co.ledger.lama.common.clients.grpc.GrpcClient
import co.ledger.lama.common.models.Coin
import co.ledger.lama.common.utils.{HexUtils, UuidUtils}
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Metadata}

trait TransactorClient {

  def createTransaction(
      accountId: UUID,
      keychainId: UUID,
      coin: Coin,
      coinSelection: CoinSelectionStrategy,
      outputs: List[PrepareTxOutput],
      feeLevel: FeeLevel,
      customFee: Option[Long],
      maxUtxos: Option[Int]
  ): IO[RawTransaction]

  def generateSignature(
      rawTransaction: RawTransaction,
      privKey: String
  ): IO[List[String]]

  def broadcastTransaction(
      keychainId: UUID,
      coinId: String,
      rawTransaction: RawTransaction,
      hexSignatures: List[String]
  ): IO[BroadcastTransaction]
}

class TransactorGrpcClient(
    val managedChannel: ManagedChannel
)(implicit val cs: ContextShift[IO])
    extends TransactorClient {

  val client: protobuf.BitcoinTransactorServiceFs2Grpc[IO, Metadata] =
    GrpcClient.resolveClient(
      protobuf.BitcoinTransactorServiceFs2Grpc.stub[IO],
      managedChannel,
      "TransactorClient"
    )

  def createTransaction(
      accountId: UUID,
      keychainId: UUID,
      coin: Coin,
      coinSelection: CoinSelectionStrategy,
      outputs: List[PrepareTxOutput],
      feeLevel: FeeLevel,
      customFee: Option[Long],
      maxUtxos: Option[Int]
  ): IO[RawTransaction] =
    client
      .createTransaction(
        new protobuf.CreateTransactionRequest(
          UuidUtils.uuidToBytes(accountId),
          UuidUtils.uuidToBytes(keychainId),
          coinSelection.toProto,
          outputs.map(_.toProto),
          coin.name,
          feeLevel.toProto,
          customFee.getOrElse(0L),
          maxUtxos.getOrElse(0)
        ),
        new Metadata
      )
      .map(RawTransaction.fromProto)

  def generateSignature(rawTransaction: RawTransaction, privKey: String): IO[List[String]] =
    client
      .generateSignatures(
        protobuf.GenerateSignaturesRequest(
          Some(rawTransaction.toProto),
          privKey
        ),
        new Metadata
      )
      .map(
        _.signatures.map(sig => HexUtils.valueOf(sig.toByteArray)).toList
      )

  def broadcastTransaction(
      keychainId: UUID,
      coinId: String,
      rawTransaction: RawTransaction,
      hexSignatures: List[String]
  ): IO[BroadcastTransaction] = {
    client
      .broadcastTransaction(
        protobuf.BroadcastTransactionRequest(
          UuidUtils.uuidToBytes(keychainId),
          coinId,
          Some(rawTransaction.toProto),
          hexSignatures.map(signature => ByteString.copyFrom(HexUtils.valueOf(signature)))
        ),
        new Metadata
      )
      .map(BroadcastTransaction.fromProto)
  }
}
