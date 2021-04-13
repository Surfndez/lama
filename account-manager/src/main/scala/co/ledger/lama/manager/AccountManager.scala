package co.ledger.lama.manager

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.manager.config.CoinConfig
import doobie.implicits._
import doobie.util.transactor.Transactor
import java.util.UUID

import co.ledger.lama.common.models._
import co.ledger.lama.manager.Exceptions._
import io.circe.JsonObject

class AccountManager(val db: Transactor[IO], val coinConfigs: List[CoinConfig])
    extends DefaultContextLogging {

  def updateAccount(
      accountId: UUID,
      label: Option[String],
      syncFrequency: Option[Long]
  ): IO[Unit] =
    (for {
      _ <- syncFrequency.map(Queries.updateAccountSyncFrequency(accountId, _)).sequence
      _ <- label.map(Queries.updateAccountLabel(accountId, _)).sequence
    } yield ()).transact(db).void

  def registerAccount(
      key: String,
      coinFamily: CoinFamily,
      coin: Coin,
      syncFrequencyO: Option[Long],
      label: Option[String],
      group: AccountGroup
  ): IO[SyncEventResult] = {

    val account = Account(
      key,
      coinFamily,
      coin,
      group
    )

    for {
      // Get the sync frequency from the request
      // or fallback to the default one from the coin configuration.
      syncFrequency <- IO.fromOption {
        syncFrequencyO orElse
          coinConfigs
            .find(c => c.coinFamily == coinFamily && c.coin == coin)
            .map(_.syncFrequency.toSeconds)
      }(CoinConfigurationException(coinFamily, coin))

      // Build queries.
      queries = for {
        // Insert the account info.
        accountInfo <- Queries
          .insertAccountInfo(
            account,
            label,
            syncFrequency
          )
        accountId     = accountInfo.account.id
        syncFrequency = accountInfo.syncFrequency

        // Create then insert the registered event.
        syncEvent = WorkableEvent[JsonObject](
          account,
          UUID.randomUUID(),
          Status.Registered,
          None,
          None,
          Instant.now()
        )

        _ <- Queries.insertSyncEvent(syncEvent)

      } yield (accountId, syncEvent.syncId, syncFrequency)

      response <-
        // Run queries and return an sync event result.
        queries
          .transact(db)
          .map { case (accountId, syncId, _) =>
            SyncEventResult(accountId, syncId)
          }
    } yield response
  }

  def resyncAccount(accountId: UUID, wipe: Boolean): IO[SyncEventResult] =
    for {
      accountInfos <- getAccountInfo(accountId)

      resyncFromCursor =
        if (wipe) None
        else accountInfos.lastSyncEvent.flatMap(_.cursor)

      // Create then insert the registered event.
      syncEvent = WorkableEvent[JsonObject](
        accountInfos.account,
        UUID.randomUUID(),
        Status.Registered,
        resyncFromCursor,
        None,
        Instant.now()
      )
      _ <- Queries.insertSyncEvent(syncEvent).transact(db)
    } yield SyncEventResult(syncEvent.account.id, syncEvent.syncId)

  def unregisterAccount(
      accountId: UUID
  ): IO[SyncEventResult] =
    for {

      existing <- Queries
        .getLastSyncEvent(accountId)
        .transact(db)
        .map(
          _.filter(e => e.status == Status.Unregistered || e.status == Status.Deleted)
        )

      result <- existing match {
        case Some(e) =>
          IO.pure(
            SyncEventResult(
              e.account.id,
              e.syncId
            )
          )

        case _ =>
          for { //TODO: refacto double for ?
            accountInfos <- getInfos(accountId)

            // Create then insert an unregistered event.
            event = WorkableEvent[JsonObject](
              accountInfos.account,
              UUID.randomUUID(),
              Status.Unregistered,
              None,
              None,
              Instant.now()
            )

            result <- Queries
              .insertSyncEvent(event)
              .transact(db)
              .map(_ => SyncEventResult(event.account.id, event.syncId))
          } yield result
      }
    } yield result

  def getAccountInfo(
      accountId: UUID
  ): IO[AccountInfo] =
    for {
      accountInfo   <- getInfos(accountId)
      lastSyncEvent <- Queries.getLastSyncEvent(accountInfo.account.id).transact(db)
    } yield {
      AccountInfo(
        accountInfo.account,
        accountInfo.syncFrequency,
        lastSyncEvent,
        accountInfo.label
      )
    }

  private def getInfos(accountId: UUID) = {
    Queries
      .getAccountInfo(accountId)
      .transact(db)
      .flatMap {
        IO.fromOption(_)(AccountNotFoundException(accountId))
      }
  }

  def getAccounts(
      group: Option[AccountGroup],
      requestLimit: Int,
      requestOffset: Int
  ): IO[AccountsResult] = {
    val limit  = if (requestLimit <= 0) 20 else requestLimit
    val offset = if (requestOffset < 0) 0 else requestOffset

    for {
      accounts <- Queries
        .getAccounts(
          group.map(_.name),
          offset = offset,
          limit = limit
        )
        .transact(db)
        .compile
        .toList

      total <- Queries.countAccounts().transact(db)
    } yield {
      AccountsResult(
        accounts.map(accountStatus =>
          AccountInfo(
            accountStatus.account,
            accountStatus.syncFrequency,
            Some(
              SyncEvent(
                accountStatus.account,
                accountStatus.syncId,
                accountStatus.status,
                accountStatus.cursor,
                accountStatus.error,
                accountStatus.updated
              )
            ),
            accountStatus.label
          )
        ),
        total
      )
    }
  }

  def getSyncEvents(
      accountId: UUID,
      requestLimit: Int,
      requestOffset: Int,
      sort: Sort
  ): IO[SyncEventsResult[JsonObject]] = {

    val limit  = Some(if (requestLimit <= 0) 20 else requestLimit)
    val offset = Some(if (requestOffset < 0) 0 else requestOffset)

    for {
      syncEvents <- Queries
        .getSyncEvents(accountId, sort, limit, offset)
        .transact(db)
        .compile
        .toList

      total <- Queries.countSyncEvents(accountId).transact(db)
    } yield {
      SyncEventsResult(
        syncEvents,
        total
      )
    }
  }

}
