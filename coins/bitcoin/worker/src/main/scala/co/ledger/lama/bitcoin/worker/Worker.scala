package co.ledger.lama.bitcoin.worker

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import co.ledger.lama.bitcoin.common.clients.grpc.{InterpreterClient, KeychainClient}
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.models.explorer.{
  Block,
  ConfirmedTransaction,
  UnconfirmedTransaction
}
import co.ledger.lama.bitcoin.worker.config.Config
import co.ledger.lama.bitcoin.worker.services._
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models.Status.{Registered, Unregistered}
import co.ledger.lama.common.models.messages.{ReportMessage, WorkerMessage}
import co.ledger.lama.common.models.{Coin, ReportError, ReportableEvent}
import fs2.Stream
import io.circe.syntax._

import java.util.UUID
import scala.util.Try

class Worker(
    syncEventService: SyncEventService,
    keychainClient: KeychainClient,
    explorerClient: Coin => ExplorerClient,
    interpreterClient: InterpreterClient,
    cursorStateService: Coin => CursorStateService,
    conf: Config
) extends IOLogging {

  val bookkeeper = new Bookkeeper(
    new Keychain(keychainClient),
    explorerClient,
    interpreterClient,
    conf.maxTxsToSavePerBatch,
    conf.maxConcurrent
  )

  def run(implicit cs: ContextShift[IO], t: Timer[IO]): Stream[IO, Unit] =
    syncEventService.consumeWorkerMessages
      .evalMap { message =>
        val reportableEvent =
          message.event.status match {
            case Registered   => synchronizeAccount(message)
            case Unregistered => deleteAccount(message)
          }

        // In case of error, fallback to a reportable failed event.
        log.info(s"Received message: ${message.asJson.toString}") *>
          reportableEvent
            .handleErrorWith { error =>
              val failedEvent = message.event.asReportableFailureEvent(
                ReportError.fromThrowable(error)
              )

              log.error(s"Failed event: $failedEvent", error) *>
                IO.pure(failedEvent)
            }
            // Always report the event within a message at the end.
            .flatMap { reportableEvent =>
              syncEventService.reportMessage(
                ReportMessage(
                  account = message.account,
                  event = reportableEvent
                )
              )
            }
      }

  def synchronizeAccount(
      workerMessage: WorkerMessage[Block]
  )(implicit cs: ContextShift[IO], t: Timer[IO]): IO[ReportableEvent[Block]] = {
    val account       = workerMessage.account
    val workableEvent = workerMessage.event

    val previousBlockState = workableEvent.cursor

    // sync the whole account per streamed batch
    for {
      _          <- log.info(s"Syncing from cursor state: $previousBlockState")
      keychainId <- IO.fromTry(Try(UUID.fromString(account.key)))

      // REORG
      lastValidBlock <- previousBlockState.map { block =>
        for {
          lvb <- cursorStateService(account.coin).getLastValidState(account.id, block)
          _   <- log.info(s"Last valid block : $lvb")
          _ <-
            if (lvb.hash.endsWith(block.hash))
              // If the previous block is still valid, do not reorg
              IO.unit
            else {
              // remove all transactions and operations up until last valid block
              log.info(
                s"${block.hash} is different than ${lvb.hash}, reorg is happening"
              ) *> interpreterClient.removeDataFromCursor(account.id, Some(lvb.height))
            }
        } yield lvb
      }.sequence

      addressesUsedByMempool <- bookkeeper
        .record[UnconfirmedTransaction](
          account.coin,
          account.id,
          keychainId,
          None
        )
        .flatMap(r => Stream.emits(r.addresses))
        .compile
        .toList

      batchResults <- bookkeeper
        .record[ConfirmedTransaction](
          account.coin,
          account.id,
          keychainId,
          lastValidBlock.map(_.hash)
        )
        .compile
        .toList

      addresses = batchResults.flatMap(_.addresses)

      txs = batchResults.flatMap(_.transactions).distinctBy(_.hash)

      lastBlock =
        if (txs.isEmpty) previousBlockState
        else Some(txs.maxBy(_.block.time).block)

      _ <- log.info(s"New cursor state: $lastBlock")

      opsCount <- interpreterClient.compute(
        account.id,
        account.coin,
        (addresses ++ addressesUsedByMempool).distinct,
        lastBlock.map(_.height)
      )

      _ <- log.info(s"$opsCount operations computed")

    } yield {
      // Create the reportable successful event.
      workableEvent.asReportableSuccessEvent(lastBlock)
    }
  }

  def deleteAccount(message: WorkerMessage[Block]): IO[ReportableEvent[Block]] =
    interpreterClient
      .removeDataFromCursor(message.account.id, None)
      .map(_ => message.event.asReportableSuccessEvent(None))
}
