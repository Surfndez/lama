package co.ledger.lama.scheduler

import cats.effect.{ConcurrentEffect, IO}
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.common.models.{Account, AccountGroup, Sort}
import co.ledger.lama.common.utils.UuidUtils
import co.ledger.lama.scheduler.protobuf.{ResyncAccountRequest, SyncEventResult}
import com.google.protobuf.empty.Empty
import io.grpc.{Metadata, ServerServiceDefinition, Status}

trait AccountManagerService extends protobuf.AccountManagerServiceFs2Grpc[IO, Metadata] {

  def definition(implicit ce: ConcurrentEffect[IO]): ServerServiceDefinition =
    protobuf.AccountManagerServiceFs2Grpc.bindService(this)

}

class AccountManagerGrpcService(accountManager: AccountManager)
    extends AccountManagerService
    with DefaultContextLogging {

  def updateAccount(request: protobuf.UpdateAccountRequest, ctx: Metadata): IO[Empty] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      label         = request.field.label
      syncFrequency = request.field.syncFrequency
      _ <- accountManager.updateAccount(accountId, label, syncFrequency)
    } yield Empty()
  }

  def registerAccount(
      request: protobuf.RegisterAccountRequest,
      ctx: Metadata
  ): IO[protobuf.SyncEventResult] = {

    // TODO: make Option in request
    val syncFrequencyO = if (request.syncFrequency == 0L) None else Some(request.syncFrequency)

    request.account match {

      case Some(account) =>
        accountManager
          .registerAccount(
            Account.fromProto(account),
            syncFrequencyO,
            request.label.map(_.value)
          )
          .map(_.toProto)

      case None =>
        // TODO: Add metadata about the actual INVALID_ARGUMENT in the IO
        log.error("received an account registration without group field.") *> IO.raiseError(
          Status.INVALID_ARGUMENT.asException()
        )
    }
  }

  def resyncAccount(request: ResyncAccountRequest, ctx: Metadata): IO[SyncEventResult] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      response  <- accountManager.resyncAccount(accountId, request.wipe)
    } yield response.toProto

  def unregisterAccount(
      request: protobuf.UnregisterAccountRequest,
      ctx: Metadata
  ): IO[protobuf.SyncEventResult] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      response  <- accountManager.unregisterAccount(accountId)
    } yield response.toProto

  def getAccountInfo(
      request: protobuf.AccountInfoRequest,
      ctx: Metadata
  ): IO[protobuf.AccountInfoResult] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      response  <- accountManager.getAccountInfo(accountId)
    } yield response.toProto

  def getAccounts(
      request: protobuf.GetAccountsRequest,
      ctx: Metadata
  ): IO[protobuf.AccountsResult] =
    accountManager
      .getAccounts(request.group.map(AccountGroup.fromProto), request.limit, request.offset)
      .map(_.toProto)

  def getSyncEvents(
      request: protobuf.GetSyncEventsRequest,
      ctx: Metadata
  ): IO[protobuf.GetSyncEventsResult] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      sort = Sort.fromIsAsc(request.sort.isAsc)
      response <- accountManager.getSyncEvents(accountId, request.limit, request.offset, sort)
    } yield response.toProto
}
