package co.ledger.lama.scheduler

import cats.effect.IO
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.common.utils.IOAssertion
import co.ledger.lama.common.utils.rabbitmq.{AutoAckMessage, RabbitUtils}
import co.ledger.lama.scheduler.config.NotifierConfig
import co.ledger.lama.scheduler.config.config.CoinConfig
import co.ledger.lama.scheduler.domain.adapters.secondary.notifier.lama.RabbitNotifier
import co.ledger.lama.scheduler.domain.adapters.secondary.persistence.Queries
import co.ledger.lama.scheduler.domain.adapters.secondary.queue.RedisPublishingQueue
import co.ledger.lama.scheduler.domain.models._
import co.ledger.lama.scheduler.domain.services.{AccountManager, CoinOrchestrator, Notifier, PublishingQueue}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeName
import doobie.implicits._
import fs2.Stream
import io.circe.{Json, JsonObject}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration._

class AccountManagerIT
    extends AnyFlatSpecLike
    with Matchers
    with TestResources
    with DefaultContextLogging {

  IOAssertion {
    setup() *>
      appResources.use { case (db, redisClient, rabbitClient) =>
        val outExchangeName = conf.notifier.asInstanceOf[NotifierConfig.Lama].exchangeName

        def mkNotifier(coinConfig: CoinConfig): Notifier = new RabbitNotifier(rabbitClient, outExchangeName, coinConfig.routingKey)

        def mkPublishingQueue(publish: WorkableEvent[JsonObject] => IO[Unit]): PublishingQueue[UUID, WorkableEvent[JsonObject]] =
          new RedisPublishingQueue[UUID, WorkableEvent[JsonObject]](publish, redisClient)

        def mkEventStream(coinConfig: CoinConfig): Stream[IO, AutoAckMessage[ReportableEvent[JsonObject]]] =
          RabbitUtils
            .createConsumer[ReportableEvent[JsonObject]](
              rabbitClient,
              coinConfig.queueName(conf.orchestrator.lamaEventsExchangeName)
            )


        val service = new AccountManager(db, conf.orchestrator.coins)

        val coinOrchestrator =
          new CoinOrchestrator(conf.orchestrator, db, mkEventStream, mkNotifier, mkPublishingQueue)

        val worker = new SimpleWorker(
          rabbitClient,
          outExchangeName,
          conf.orchestrator.lamaEventsExchangeName,
          conf.orchestrator.coins.head
        )

        val nbEvents = 15
        val account =
          Account(accountTest.identifier, CoinFamily.Bitcoin, Coin.Btc, AccountGroup("TestGroup"))

        def runTests(): IO[Unit] =
          for {
            // Register an account.
            registeredResult <- service.registerAccount(
              account,
              None,
              None
            )

            registeredAccountId = registeredResult.accountId
            registeredSyncId    = registeredResult.syncId

            eventSent1 <- worker.consumeWorkerEvent()

            // Report a successful sync event with a new cursor.
            syncedCursorJson = Json.obj("blockHeight" -> Json.fromLong(123456789)).asObject
            _ <- worker.publishReportEvent(
              eventSent1.asReportableSuccessEvent(syncedCursorJson)
            )

            eventSent2 <- worker.consumeWorkerEvent()

            // Report a failed sync event with an error message.
            syncFailedError = ReportError(code = "sync_failed", message = Some("failed to sync"))
            _ <- worker.publishReportEvent(
              eventSent2.asReportableFailureEvent(syncFailedError)
            )

            // reseed scenario
            _ <- service.resyncAccount(accountTest.id, wipe = true)

            eventSent3 <- worker.consumeWorkerEvent()

            // Report after a reseed a successful sync event
            _ <- worker.publishReportEvent(
              eventSent3.asReportableSuccessEvent(syncedCursorJson)
            )

            // Unregister an account.
            unregisteredResult <- service.unregisterAccount(accountTest.id)

            unregisteredAccountId = unregisteredResult.accountId
            unregisteredSyncId    = unregisteredResult.syncId

            eventSent4 <- worker.consumeWorkerEvent()

            // Report a failed delete event with an error message.
            deleteFailedError = ReportError(
              code = "delete_failed",
              message = Some("failed to delete data")
            )
            _ <- worker.publishReportEvent(
              eventSent4.asReportableFailureEvent(deleteFailedError)
            )

            eventSent5 <- worker.consumeWorkerEvent()

            // Report a successful delete event.
            _ <- worker.publishReportEvent(
              eventSent5.asReportableSuccessEvent(None)
            )

            // Fetch all sync events.
            syncEvents <- Queries
              .getSyncEvents(accountTest.id, Sort.Ascending)
              .take(nbEvents)
              .compile
              .toList
              .transact(db)
          } yield {
            it should "have consumed events from worker" in {
              eventSent1 shouldBe WorkableEvent(
                accountTest,
                registeredSyncId,
                Status.Registered,
                None,
                None,
                eventSent1.time
              )

              eventSent2 shouldBe
                WorkableEvent(
                  accountTest,
                  eventSent2.syncId,
                  Status.Registered,
                  syncedCursorJson,
                  None,
                  eventSent2.time
                )

              eventSent3 shouldBe
                WorkableEvent(
                  accountTest,
                  eventSent3.syncId,
                  Status.Registered,
                  None,
                  None,
                  eventSent3.time
                )

              eventSent4 shouldBe
                WorkableEvent(
                  accountTest,
                  unregisteredSyncId,
                  Status.Unregistered,
                  None,
                  None,
                  eventSent4.time
                )

              eventSent5 shouldBe
                WorkableEvent(
                  accountTest,
                  eventSent5.syncId,
                  Status.Unregistered,
                  None,
                  Some(deleteFailedError),
                  eventSent5.time
                )

            }

            it should s"have $nbEvents inserted events" in {
              syncEvents should have size nbEvents
            }

            it should "succeed to register an account" in {
              registeredAccountId shouldBe accountTest.id
            }

            it should "have (registered -> published -> synchronized) events for the first iteration" in {
              val eventsBatch1 = syncEvents.slice(0, 3)
              eventsBatch1 shouldBe List(
                WorkableEvent(
                  accountTest,
                  registeredSyncId,
                  Status.Registered,
                  None,
                  None,
                  eventsBatch1.head.time
                ),
                FlaggedEvent(
                  accountTest,
                  registeredSyncId,
                  Status.Published,
                  None,
                  None,
                  eventsBatch1(1).time
                ),
                ReportableEvent(
                  accountTest,
                  registeredSyncId,
                  Status.Synchronized,
                  syncedCursorJson,
                  None,
                  eventsBatch1(2).time
                )
              )
            }

            it should "have (registered -> published -> sync_failed) events for the next iteration" in {
              val eventsBatch2 = syncEvents.slice(3, 6)
              eventsBatch2 shouldBe List(
                WorkableEvent(
                  accountTest,
                  eventSent2.syncId,
                  Status.Registered,
                  syncedCursorJson,
                  None,
                  eventsBatch2.head.time
                ),
                FlaggedEvent(
                  accountTest,
                  eventSent2.syncId,
                  Status.Published,
                  syncedCursorJson,
                  None,
                  eventsBatch2(1).time
                ),
                ReportableEvent(
                  accountTest,
                  eventSent2.syncId,
                  Status.SyncFailed,
                  syncedCursorJson,
                  Some(syncFailedError),
                  eventsBatch2(2).time
                )
              )
            }

            it should "have (registered -> published -> synchronized) events after a reseed (force sync manual from 0)" in {
              val eventsBatch3 = syncEvents.slice(6, 9)
              eventsBatch3 shouldBe List(
                WorkableEvent(
                  accountTest,
                  eventSent3.syncId,
                  Status.Registered,
                  None,
                  None,
                  eventsBatch3.head.time
                ),
                FlaggedEvent(
                  accountTest,
                  eventSent3.syncId,
                  Status.Published,
                  None,
                  None,
                  eventsBatch3(1).time
                ),
                ReportableEvent(
                  accountTest,
                  eventSent3.syncId,
                  Status.Synchronized,
                  syncedCursorJson,
                  None,
                  eventsBatch3(2).time
                )
              )
            }

            it should "succeed to unregister an account" in {
              unregisteredAccountId shouldBe accountTest.id
            }

            it should "have (unregistered -> published -> delete_failed) events for the next iteration" in {
              val eventsBatch4 = syncEvents.slice(9, 12)
              eventsBatch4 shouldBe List(
                WorkableEvent(
                  accountTest,
                  eventSent4.syncId,
                  Status.Unregistered,
                  None,
                  None,
                  eventsBatch4.head.time
                ),
                FlaggedEvent(
                  accountTest,
                  eventSent4.syncId,
                  Status.Published,
                  None,
                  None,
                  eventsBatch4(1).time
                ),
                ReportableEvent(
                  accountTest,
                  eventSent4.syncId,
                  Status.DeleteFailed,
                  None,
                  Some(deleteFailedError),
                  eventsBatch4(2).time
                )
              )
            }

            it should "have (unregistered -> published -> deleted) events at the end" in {
              val eventsBatch5 = syncEvents.slice(12, 15)
              eventsBatch5 shouldBe List(
                WorkableEvent(
                  accountTest,
                  eventSent5.syncId,
                  Status.Unregistered,
                  None,
                  Some(deleteFailedError),
                  eventsBatch5.head.time
                ),
                FlaggedEvent(
                  accountTest,
                  eventSent5.syncId,
                  Status.Published,
                  None,
                  Some(deleteFailedError),
                  eventsBatch5(1).time
                ),
                ReportableEvent(
                  accountTest,
                  eventSent5.syncId,
                  Status.Deleted,
                  None,
                  None,
                  eventsBatch5(2).time
                )
              )
            }
          }

        coinOrchestrator
          .run(stopAtNbTick = Some(nbEvents + 1)) // run the orchestrator
          .concurrently(Stream.eval(runTests()))  // and run tests at the same time
          .timeout(5.minutes)
          .compile
          .drain
      }
  }

}


class SimpleWorker(
                    rabbit: RabbitClient[IO],
                    inExchangeName: ExchangeName,
                    outExchangeName: ExchangeName,
                    coinConf: CoinConfig
                  ) {

  private val consumer: Stream[IO, WorkableEvent[JsonObject]] =
    RabbitUtils
      .createAutoAckConsumer[WorkableEvent[JsonObject]](rabbit, coinConf.queueName(inExchangeName))

  private val publisher: Stream[IO, ReportableEvent[JsonObject] => IO[Unit]] =
    RabbitUtils
      .createPublisher[ReportableEvent[JsonObject]](rabbit, outExchangeName, coinConf.routingKey)

  def consumeWorkerEvent(): IO[WorkableEvent[JsonObject]] =
    consumer.take(1).compile.last.map(_.get)

  def publishReportEvent(event: ReportableEvent[JsonObject]): IO[Unit] =
    publisher.evalMap(p => p(event)).compile.drain

}
