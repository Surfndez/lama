package co.ledger.lama.bitcoin.common.clients.grpc

import cats.effect.IO
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.common.utils.BtcProtoUtils._
import co.ledger.lama.bitcoin.interpreter.protobuf
import co.ledger.lama.common.clients.grpc.{GrpcClient, GrpcClientResource}
import co.ledger.lama.common.models.{Account, Sort}
import co.ledger.lama.common.utils.{TimestampProtoUtils, UuidUtils}
import io.grpc.Metadata
import fs2._

import java.time.Instant
import java.util.UUID

trait InterpreterClient {
  def saveTransactions(accountId: UUID): Pipe[IO, TransactionView, Unit]

  def removeDataFromCursor(
      accountId: UUID,
      blockHeightCursor: Option[Long],
      followUpId: UUID
  ): IO[Int]

  def getLastBlocks(accountId: UUID): IO[List[BlockView]]

  def compute(
      account: Account,
      syncId: UUID,
      addresses: List[AccountAddress]
  ): IO[Int]

  def getOperations(
      accountId: UUID,
      limit: Int,
      sort: Option[Sort],
      cursor: Option[String]
  ): IO[GetOperationsResult]

  def getOperation(
      accountId: UUID,
      operationId: String
  ): IO[Option[Operation]]

  def getUtxos(
      accountId: UUID,
      limit: Int,
      offset: Int,
      sort: Option[Sort]
  ): IO[GetUtxosResult]

  def getUnconfirmedUtxos(
      accountId: UUID
  ): IO[List[Utxo]]

  def getBalance(accountId: UUID): IO[CurrentBalance]

  def getBalanceHistory(
      accountId: UUID,
      start: Option[Instant],
      end: Option[Instant],
      interval: Option[Int]
  ): IO[GetBalanceHistoryResult]

}

class InterpreterGrpcClient(grpcClientResource: GrpcClientResource) extends InterpreterClient {

  val client: protobuf.BitcoinInterpreterServiceFs2Grpc[IO, Metadata] =
    GrpcClient.resolveClient(
      protobuf.BitcoinInterpreterServiceFs2Grpc.stub[IO],
      grpcClientResource.dispatcher,
      grpcClientResource.channel,
      "InterpreterClient"
    )

  def saveTransactions(accountId: UUID): Pipe[IO, TransactionView, Unit] =
    _.map { tx =>
      protobuf.SaveTransactionRequest(
        accountId = UuidUtils.uuidToBytes(accountId),
        transaction = Some(tx.toProto)
      )
    }.through(client.saveTransactions(_, new Metadata()).as(()))

  def removeDataFromCursor(
      accountId: UUID,
      blockHeightCursor: Option[Long],
      followUpId: UUID
  ): IO[Int] =
    client
      .removeDataFromCursor(
        protobuf.DeleteTransactionsRequest(
          UuidUtils uuidToBytes accountId,
          blockHeightCursor.getOrElse(0),
          UuidUtils.uuidToBytes(followUpId)
        ),
        new Metadata()
      )
      .map(_.count)

  def getLastBlocks(accountId: UUID): IO[List[BlockView]] =
    client
      .getLastBlocks(
        protobuf.GetLastBlocksRequest(
          UuidUtils uuidToBytes accountId
        ),
        new Metadata()
      )
      .map(_.blocks.map(BlockView.fromProto).toList)

  def compute(
      account: Account,
      syncId: UUID,
      addresses: List[AccountAddress]
  ): IO[Int] =
    client
      .compute(
        protobuf.ComputeRequest(
          Some(account.toBtcProto),
          UuidUtils.uuidToBytes(syncId),
          addresses.map(_.toProto)
        ),
        new Metadata()
      )
      .map(_.count)

  def getOperations(
      accountId: UUID,
      limit: Int,
      sort: Option[Sort],
      cursor: Option[String]
  ): IO[GetOperationsResult] =
    client
      .getOperations(
        protobuf.GetOperationsRequest(
          accountId = UuidUtils.uuidToBytes(accountId),
          limit = limit,
          sort = sort.map(_.toProto).getOrElse(protobuf.SortingOrder.DESC),
          cursor = cursor.getOrElse("")
        ),
        new Metadata
      )
      .map(GetOperationsResult.fromProto)

  def getOperation(
      accountId: UUID,
      operationId: String
  ): IO[Option[Operation]] =
    client
      .getOperation(
        protobuf.GetOperationRequest(UuidUtils.uuidToBytes(accountId), operationId),
        new Metadata
      )
      .map(_.operation.map(Operation.fromProto))

  def getUtxos(
      accountId: UUID,
      limit: Int,
      offset: Int,
      sort: Option[Sort]
  ): IO[GetUtxosResult] = {
    client
      .getUtxos(
        protobuf.GetUtxosRequest(
          accountId = UuidUtils.uuidToBytes(accountId),
          limit = limit,
          offset = offset,
          sort = sort.map(_.toProto).getOrElse(protobuf.SortingOrder.DESC)
        ),
        new Metadata
      )
      .map(GetUtxosResult.fromProto)
  }

  def getUnconfirmedUtxos(
      accountId: UUID
  ): IO[List[Utxo]] = {
    client
      .getUnconfirmedUtxos(
        protobuf.GetUnconfirmedUtxosRequest(
          accountId = UuidUtils.uuidToBytes(accountId)
        ),
        new Metadata
      )
      .map(_.utxos.map(Utxo.fromProto).toList)
  }

  def getBalance(accountId: UUID): IO[CurrentBalance] = {
    client
      .getBalance(
        protobuf.GetBalanceRequest(
          accountId = UuidUtils.uuidToBytes(accountId)
        ),
        new Metadata
      )
      .map(CurrentBalance.fromProto)
  }

  def getBalanceHistory(
      accountId: UUID,
      start: Option[Instant],
      end: Option[Instant],
      interval: Option[Int]
  ): IO[GetBalanceHistoryResult] = {
    client
      .getBalanceHistory(
        protobuf.GetBalanceHistoryRequest(
          accountId = UuidUtils.uuidToBytes(accountId),
          start.map(TimestampProtoUtils.serialize),
          end.map(TimestampProtoUtils.serialize),
          interval.getOrElse(0)
        ),
        new Metadata
      )
      .map(GetBalanceHistoryResult.fromProto)
  }
}
