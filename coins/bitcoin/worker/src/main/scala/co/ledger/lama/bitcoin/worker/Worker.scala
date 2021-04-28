package co.ledger.lama.bitcoin.worker

import cats.effect.IO
import cats.implicits._
import co.ledger.lama.bitcoin.common.clients.grpc.{InterpreterClient, KeychainClient}
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.models.explorer.{
  Block,
  ConfirmedTransaction,
  UnconfirmedTransaction
}
import co.ledger.lama.bitcoin.worker.services._
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.models.Status.{Registered, Unregistered}
import co.ledger.lama.common.models._
import fs2.Stream
import io.circe.syntax._

import java.util.UUID
import scala.math.Ordering.Implicits._
import scala.util.Try

class Worker(
    syncEventService: SyncEventService,
    keychainClient: KeychainClient,
    explorerClient: Coin => ExplorerClient,
    interpreterClient: InterpreterClient,
    cursorService: Coin => CursorStateService[IO]
) extends ContextLogging {

  def run: Stream[IO, Unit] =
    syncEventService.consumeWorkerEvents
      .evalMap { autoAckMsg =>
        autoAckMsg.unwrap { event =>
          implicit val lc: LamaLogContext =
            LamaLogContext().withAccount(event.account).withFollowUpId(event.syncId)

          val reportableEvent = event.status match {
            case Registered   => synchronizeAccount(event)
            case Unregistered => deleteAccount(event)
          }

          // In case of error, fallback to a reportable failed event.
          log.info(s"Received event: ${event.asJson.toString}") *>
            reportableEvent
              .handleErrorWith { error =>
                val failedEvent = event.asReportableFailureEvent(
                  ReportError.fromThrowable(error)
                )

                log.error(s"Failed event: $failedEvent", error) *>
                  IO.pure(failedEvent)
              }
              // Always report the event at the end.
              .flatMap { reportableEvent =>
                syncEventService.reportEvent(reportableEvent)
              }
        }
      }

  def synchronizeAccount(
      workerEvent: WorkableEvent[Block]
  )(implicit lc: LamaLogContext): IO[ReportableEvent[Block]] = {

    val bookkeeper = Bookkeeper(
      new Keychain(keychainClient),
      explorerClient,
      interpreterClient
    )

    val account            = workerEvent.account
    val previousBlockState = workerEvent.cursor

    // sync the whole account per streamed batch
    for {

      keychainId <- IO.fromTry(Try(UUID.fromString(account.identifier)))

      addressesUsedByMempool <- bookkeeper
        .record[UnconfirmedTransaction](
          account.coin,
          account.id,
          keychainId,
          None
        )

      lastMinedBlock <- lastMinedBlock(account.coin)

      addresses <- Stream
        .emit(previousBlockState)
        .filter {
          case Some(previous) => previous < lastMinedBlock.block
          case None           => true
        }
        .evalTap(b => log.info(s"Syncing from cursor state: $b"))
        .evalMap(b => b.map(rewindToLastValidBlock(account, _, workerEvent.syncId)).sequence)
        .evalMap { lastValidBlock =>
          bookkeeper
            .record[ConfirmedTransaction](
              account.coin,
              account.id,
              keychainId,
              lastValidBlock.map(_.hash)
            )
        }
        .compile
        .toList
        .map(_.flatten)

      _ <- log.info(s"New cursor state: ${lastMinedBlock.block}")

      opsCount <- interpreterClient.compute(
        account,
        workerEvent.syncId,
        (addresses ++ addressesUsedByMempool).distinct
      )

      _ <- log.info(s"$opsCount operations computed")

    } yield {
      // Create the reportable successful event.
      workerEvent.asReportableSuccessEvent(Some(lastMinedBlock.block))
    }
  }

  case class LastMinedBlock(block: Block)

  def lastMinedBlock(coin: Coin)(implicit lc: LamaLogContext): IO[LastMinedBlock] =
    explorerClient(coin).getCurrentBlock.map(LastMinedBlock)

  private def rewindToLastValidBlock(account: Account, lastKnownBlock: Block, syncId: UUID)(implicit
      lc: LamaLogContext
  ): IO[Block] =
    for {

      lvb <- cursorService(account.coin).getLastValidState(account, lastKnownBlock, syncId)

      _ <- log.info(s"Last valid block : $lvb")
      _ <-
        if (lvb.hash == lastKnownBlock.hash)
          // If the previous block is still valid, do not reorg
          IO.unit
        else {
          // remove all transactions and operations up until last valid block
          log.info(
            s"${lastKnownBlock.hash} is different than ${lvb.hash}, reorg is happening"
          ) *> interpreterClient.removeDataFromCursor(account.id, Some(lvb.height), syncId)
        }
    } yield lvb

  def deleteAccount(
      event: WorkableEvent[Block]
  )(implicit lc: LamaLogContext): IO[ReportableEvent[Block]] = {
    log.info("Delete Account") *>
      interpreterClient
        .removeDataFromCursor(event.account.id, None, event.syncId)
        .map(_ => event.asReportableSuccessEvent(None))
  }
}
