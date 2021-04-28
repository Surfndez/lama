package co.ledger.lama.bitcoin.interpreter

import cats.effect.{ConcurrentEffect, IO}
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.common.utils.BtcProtoUtils._
import co.ledger.lama.bitcoin.interpreter.models.AccountTxView
import co.ledger.lama.bitcoin.interpreter.protobuf.SaveTransactionRequest
import co.ledger.lama.common.models.PaginationToken
import co.ledger.lama.common.utils.{TimestampProtoUtils, UuidUtils}
import io.grpc.{Metadata, ServerServiceDefinition}
import fs2.Stream

trait InterpreterService extends protobuf.BitcoinInterpreterServiceFs2Grpc[IO, Metadata] {
  def definition(implicit ce: ConcurrentEffect[IO]): ServerServiceDefinition =
    protobuf.BitcoinInterpreterServiceFs2Grpc.bindService(this)
}

class InterpreterGrpcService(
    interpreter: Interpreter
) extends InterpreterService {

  def saveTransactions(
      request: Stream[IO, SaveTransactionRequest],
      ctx: Metadata
  ): Stream[IO, com.google.protobuf.empty.Empty] =
    request
      .evalMap { r =>
        for {
          accountId <- UuidUtils.bytesToUuidIO(r.accountId)
          tx <- IO
            .fromOption(r.transaction)(new IllegalArgumentException("Missing transaction to save"))
            .map(TransactionView.fromProto)
        } yield AccountTxView(accountId, tx)
      }
      .through(interpreter.saveTransactions)
      .as(com.google.protobuf.empty.Empty())

  def getLastBlocks(
      request: protobuf.GetLastBlocksRequest,
      ctx: Metadata
  ): IO[protobuf.GetLastBlocksResult] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      blocks    <- interpreter.getLastBlocks(accountId)
    } yield protobuf.GetLastBlocksResult(blocks.map(_.toProto))
  }

  def getOperations(
      request: protobuf.GetOperationsRequest,
      ctx: Metadata
  ): IO[protobuf.GetOperationsResult] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      sort   = Sort.fromProto(request.sort)
      cursor = PaginationToken.fromBase64[OperationPaginationState](request.cursor)
      opResult <- interpreter.getOperations(
        accountId,
        request.limit,
        sort,
        cursor
      )
    } yield opResult.toProto
  }

  def getOperation(
      request: protobuf.GetOperationRequest,
      ctx: Metadata
  ): IO[protobuf.GetOperationResult] = {
    for {
      accountId   <- UuidUtils.bytesToUuidIO(request.accountId).map(Operation.AccountId)
      operationId <- IO.pure(Operation.UID(request.operationUid))
      operation   <- interpreter.getOperation(accountId, operationId)
    } yield protobuf.GetOperationResult(operation.map(_.toProto))
  }

  def getUtxos(request: protobuf.GetUtxosRequest, ctx: Metadata): IO[protobuf.GetUtxosResult] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      sort = Sort.fromProto(request.sort)
      res <- interpreter.getUtxos(accountId, request.limit, request.offset, sort)
    } yield {
      res.toProto
    }
  }

  def getUnconfirmedUtxos(
      request: protobuf.GetUnconfirmedUtxosRequest,
      ctx: Metadata
  ): IO[protobuf.GetUnconfirmedUtxosResult] =
    for {
      accountId        <- UuidUtils.bytesToUuidIO(request.accountId)
      unconfirmedUtxos <- interpreter.getUnconfirmedUtxos(accountId)
    } yield {
      protobuf.GetUnconfirmedUtxosResult(unconfirmedUtxos.map(_.toProto))
    }

  def removeDataFromCursor(
      request: protobuf.DeleteTransactionsRequest,
      ctx: Metadata
  ): IO[protobuf.ResultCount] = {
    for {
      accountId  <- UuidUtils.bytesToUuidIO(request.accountId)
      followUpId <- UuidUtils.bytesToUuidIO(request.followUpId)
      blockHeight = request.blockHeight
      txRes <- interpreter.removeDataFromCursor(accountId, blockHeight, followUpId)
    } yield protobuf.ResultCount(txRes)
  }

  def compute(
      request: protobuf.ComputeRequest,
      ctx: Metadata
  ): IO[protobuf.ResultCount] =
    for {
      account   <- IO(BtcAccount.fromBtcProto(request.account.get))
      syncId    <- UuidUtils.bytesToUuidIO(request.syncId)
      addresses <- IO(request.addresses.map(AccountAddress.fromProto).toList)
      nbOps     <- interpreter.compute(account, syncId, addresses)
    } yield protobuf.ResultCount(nbOps)

  def getBalance(
      request: protobuf.GetBalanceRequest,
      ctx: Metadata
  ): IO[protobuf.CurrentBalance] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      info      <- interpreter.getBalance(accountId)
    } yield info.toProto

  def getBalanceHistory(
      request: protobuf.GetBalanceHistoryRequest,
      ctx: Metadata
  ): IO[protobuf.GetBalanceHistoryResult] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      start = request.start.map(TimestampProtoUtils.deserialize)
      end   = request.end.map(TimestampProtoUtils.deserialize)
      balances <- interpreter.getBalanceHistory(accountId, start, end, request.interval)
    } yield protobuf.GetBalanceHistoryResult(balances.map(_.toProto))
}
