package co.ledger.lama.bitcoin.transactor

import cats.data.{NonEmptyList, Validated}
import cats.effect.{ConcurrentEffect, IO}
import co.ledger.lama.bitcoin.common.models.{Address, InvalidAddress}
import co.ledger.lama.bitcoin.common.models.transactor.{
  CoinSelectionStrategy,
  FeeLevel,
  PrepareTxOutput,
  RawTransaction
}
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models.{BitcoinLikeCoin, Coin}
import co.ledger.lama.common.utils.UuidUtils
import com.google.protobuf.ByteString
import io.grpc.{Metadata, ServerServiceDefinition}

trait TransactorService extends protobuf.BitcoinTransactorServiceFs2Grpc[IO, Metadata] {
  def definition(implicit ce: ConcurrentEffect[IO]): ServerServiceDefinition =
    protobuf.BitcoinTransactorServiceFs2Grpc.bindService(this)
}

class TransactorGrpcService(transactor: Transactor) extends TransactorService with IOLogging {

  def createTransaction(
      request: protobuf.CreateTransactionRequest,
      ctx: io.grpc.Metadata
  ): IO[protobuf.CreateTransactionResponse] =
    for {

      keychainId <- UuidUtils.bytesToUuidIO(request.keychainId)
      accountId  <- UuidUtils.bytesToUuidIO(request.accountId)
      coin       <- BitcoinLikeCoin.fromKeyIO(request.coinId)
      outputs       = request.outputs.map(PrepareTxOutput.fromProto).toList
      coinSelection = CoinSelectionStrategy.fromProto(request.coinSelection)
      feeLevel      = FeeLevel.fromProto(request.feeLevel)
      optCustomFee  = if (request.customFee > 0L) Some(request.customFee) else None

      _ <- log.info(
        s"""Preparing transaction:
            - accountId: $accountId
            - strategy: ${coinSelection.name}
            - coin: ${coin.name}
            - feeLevel: $feeLevel
            - customFee: $optCustomFee
            - feeLevel: $feeLevel
            - maxUtxos: ${request.maxUtxos}
         """
      )

      rawTransaction <- transactor.createTransaction(
        accountId,
        keychainId,
        outputs,
        coin,
        coinSelection,
        feeLevel,
        optCustomFee,
        request.maxUtxos
      )

    } yield {
      RawTransaction(
        rawTransaction.hex,
        rawTransaction.hash,
        rawTransaction.witnessHash,
        NonEmptyList.fromListUnsafe(rawTransaction.utxos)
      ).toProto
    }

  def generateSignatures(
      request: protobuf.GenerateSignaturesRequest,
      ctx: io.grpc.Metadata
  ): IO[protobuf.GenerateSignaturesResponse] = {
    for {

      rawTransaction <- IO.fromOption(
        request.rawTransaction.map(RawTransaction.fromProto)
      )(new Exception("Raw Transaction : bad format"))

      _ <- log.info(
        s"""Transaction to sign:
            - hex: ${rawTransaction.hex}
            - tx hash: ${rawTransaction.hash}
         """
      )

      signatures <- transactor
        .generateSignatures(rawTransaction, request.privKey)

      _ <- log.info(s"Get ${signatures.size} signatures")

    } yield protobuf.GenerateSignaturesResponse(
      signatures.map(signature => ByteString.copyFrom(signature))
    )
  }

  def broadcastTransaction(
      request: protobuf.BroadcastTransactionRequest,
      ctx: io.grpc.Metadata
  ): IO[protobuf.BroadcastTransactionResponse] =
    for {

      coin       <- Coin.fromKeyIO(request.coinId)
      keychainId <- UuidUtils.bytesToUuidIO(request.keychainId)
      rawTransaction <- IO.fromOption(
        request.rawTransaction.map(RawTransaction.fromProto)
      )(new Exception("Raw Transaction : bad format"))

      _ <- log.info(
        s"""Transaction to sign:
            - coin: ${coin.name}
            - hex: ${rawTransaction.hex}
            - tx hash: ${rawTransaction.hash}
         """
      )

      broadcastTx <- transactor.broadcastTransaction(
        rawTransaction,
        keychainId,
        request.signatures.map(_.toByteArray).toList,
        coin
      )

    } yield {
      broadcastTx.toProto
    }

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
