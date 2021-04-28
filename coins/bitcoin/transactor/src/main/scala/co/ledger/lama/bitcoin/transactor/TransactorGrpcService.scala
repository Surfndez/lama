package co.ledger.lama.bitcoin.transactor

import cats.data.Validated
import cats.effect.{IO, Resource}
import co.ledger.lama.bitcoin.common.models.interpreter.Utxo
import co.ledger.lama.bitcoin.common.models.transactor.{
  CoinSelectionStrategy,
  FeeLevel,
  PrepareTxOutput,
  RawTransaction
}
import co.ledger.lama.bitcoin.common.models.{Address, InvalidAddress}
import co.ledger.lama.common.models.{BitcoinLikeCoin, Coin}
import co.ledger.lama.common.utils.UuidUtils
import com.google.protobuf.ByteString
import io.grpc.{Metadata, ServerServiceDefinition}

trait TransactorService extends protobuf.BitcoinTransactorServiceFs2Grpc[IO, Metadata] {
  def definition: Resource[IO, ServerServiceDefinition] =
    protobuf.BitcoinTransactorServiceFs2Grpc.bindServiceResource(this)
}

class TransactorGrpcService(transactor: Transactor) extends TransactorService {

  def createTransaction(
      request: protobuf.CreateTransactionRequest,
      ctx: io.grpc.Metadata
  ): IO[protobuf.CreateTransactionResponse] =
    for {

      keychainId <- UuidUtils.bytesToUuidIO(request.keychainId)
      accountId  <- UuidUtils.bytesToUuidIO(request.accountId)
      coin       <- BitcoinLikeCoin.fromKeyIO(request.coinId)
      outputs           = request.outputs.map(PrepareTxOutput.fromProto).toList
      coinSelection     = CoinSelectionStrategy.fromProto(request.coinSelection)
      feeLevel          = FeeLevel.fromProto(request.feeLevel)
      optCustomFeePerKb = if (request.customFeePerKb > 0L) Some(request.customFeePerKb) else None

      txRes <- transactor.createTransaction(
        accountId,
        keychainId,
        outputs,
        coin,
        coinSelection,
        feeLevel,
        optCustomFeePerKb,
        request.maxUtxos
      )
    } yield txRes.toProto

  def generateSignatures(
      request: protobuf.GenerateSignaturesRequest,
      ctx: io.grpc.Metadata
  ): IO[protobuf.GenerateSignaturesResponse] = {
    for {

      rawTransaction <- IO.fromOption(
        request.rawTransaction.map(RawTransaction.fromProto)
      )(new Exception("Raw Transaction : bad format"))

      utxos = request.utxos.map(Utxo.fromProto).toList

      signatures <- transactor
        .generateSignatures(rawTransaction, utxos, request.privKey)

    } yield protobuf.GenerateSignaturesResponse(
      signatures.map(signature => ByteString.copyFrom(signature))
    )
  }

  def broadcastTransaction(
      request: protobuf.BroadcastTransactionRequest,
      ctx: io.grpc.Metadata
  ): IO[protobuf.RawTransaction] =
    for {
      coin       <- Coin.fromKeyIO(request.coinId)
      keychainId <- UuidUtils.bytesToUuidIO(request.keychainId)
      rawTransaction <- IO.fromOption(
        request.rawTransaction.map(RawTransaction.fromProto)
      )(new Exception("Raw Transaction : bad format"))

      rawTx <- transactor.broadcastTransaction(
        keychainId,
        rawTransaction,
        request.derivations.map(_.path.toList).toList,
        request.signatures.map(_.toByteArray).toList,
        coin
      )

    } yield rawTx.toProto

  override def validateAddresses(
      request: protobuf.ValidateAddressesRequest,
      ctx: Metadata
  ): IO[protobuf.ValidateAddressesResponse] = {

    def validAddress(address: String) =
      protobuf.ValidateAddressesResponse.ValidationResult.Result
        .Valid(protobuf.ValidateAddressesResponse.ValidAddress(address))

    def invalidAddress(reason: String, address: String) =
      protobuf.ValidateAddressesResponse.ValidationResult.Result
        .Invalid(protobuf.ValidateAddressesResponse.InvalidAddress(address, reason))

    for {
      coin <- IO.fromOption(Coin.fromKey(request.coinId))(
        new Throwable(s"Unknown coin ${request.coinId}")
      )
      result <- transactor.validateAddresses(coin, request.addresses.map(Address))
    } yield {

      protobuf.ValidateAddressesResponse(
        result
          .map {
            case Validated
                  .Invalid(InvalidAddress(Address(address), reason)) =>
              invalidAddress(reason, address)
            case Validated.Valid(Address(a)) => validAddress(a)
          }
          .map(protobuf.ValidateAddressesResponse.ValidationResult.of)
      )
    }
  }
}
