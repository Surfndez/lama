package co.ledger.lama.scheduler.domain.services

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits.showInterpolator
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.utils.rabbitmq.{AutoAckMessage, RabbitUtils}
import co.ledger.lama.scheduler.config.CoinConfig
import co.ledger.lama.scheduler.domain.adapters.secondary.persistence.Queries
import co.ledger.lama.scheduler.domain.models.{ReportableEvent, TriggerableEvent, WorkableEvent}
import com.redis.RedisClient
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeName
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2.{Pipe, Stream}
import io.circe.JsonObject
import io.circe.syntax._

import scala.concurrent.duration.FiniteDuration

trait SyncEventTask {

  // Source of worker events to publish.
  def publishableWorkerEvents: Stream[IO, WorkableEvent[JsonObject]]

  // Publish events pipe transformation:
  // Stream[IO, WorkableEvent[JsonObject]] => Stream[IO, Unit].
  def publishWorkerEventsPipe: Pipe[IO, WorkableEvent[JsonObject], Unit]

  // Awake every tick, source worker events then publish.
  def publishWorkerEvents(tick: FiniteDuration, stopAtNbTick: Option[Long] = None)(implicit
      t: Timer[IO]
  ): Stream[IO, Unit] =
    tickerStream(tick, stopAtNbTick) >> publishableWorkerEvents.through(publishWorkerEventsPipe)

  // Source of report events to report.
  def reportableEvents: Stream[IO, AutoAckMessage[ReportableEvent[JsonObject]]]

  // Report events pipe transformation:
  // Stream[IO, ReportableEvent[JsonObject]] => Stream[IO, Unit].
  def reportEventPipe: Pipe[IO, AutoAckMessage[ReportableEvent[JsonObject]], Unit]

  // Source reportable events then report the event of events.
  def reportEvents: Stream[IO, Unit] =
    reportableEvents.through(reportEventPipe)

  // Source of triggerable events.
  def triggerableEvents: Stream[IO, TriggerableEvent[JsonObject]]

  // Trigger events pipe transformation:
  // Stream[IO, TriggerableEvent[JsonObject]] => Stream[IO, Unit].
  def triggerEventsPipe: Pipe[IO, TriggerableEvent[JsonObject], Unit]

  // Awake every tick, source triggerable events then trigger.
  def trigger(tick: FiniteDuration)(implicit
      t: Timer[IO]
  ): Stream[IO, Unit] =
    tickerStream(tick) >> triggerableEvents.through(triggerEventsPipe)

  private def tickerStream(tick: FiniteDuration, stopAtNbTick: Option[Long] = None)(implicit
      t: Timer[IO]
  ): Stream[IO, FiniteDuration] = {
    val stream = Stream.awakeEvery[IO](tick)
    stopAtNbTick match {
      case Some(value) => stream.take(value) // useful to stop an infinite stream
      case None        => stream
    }
  }

}

class CoinSyncEventTask(
    workerExchangeName: ExchangeName,
    eventsExchangeName: ExchangeName,
    conf: CoinConfig,
    db: Transactor[IO],
    rabbit: RabbitClient[IO],
    redis: RedisClient
)(implicit cs: ContextShift[IO])
    extends SyncEventTask
    with ContextLogging {

  // Fetch worker events ready to publish from database.
  def publishableWorkerEvents: Stream[IO, WorkableEvent[JsonObject]] =
    Queries
      .fetchPublishableWorkerEvents(conf.coinFamily, conf.coin)
      .transact(db)

  // Publisher publishing to the worker exchange with routingKey = "coinFamily.coin".
  private val publisher =
    new WorkableEventPublisher(
      redis,
      rabbit,
      workerExchangeName,
      conf.routingKey
    )

  // Publish events to the worker exchange queue, mark event as published then insert.
  def publishWorkerEventsPipe: Pipe[IO, WorkableEvent[JsonObject], Unit] =
    _.evalMap { event =>
      publisher.enqueue(event) &>
        Queries
          .insertSyncEvent(event.asPublished)
          .transact(db)
          .void
    }

  // Consume events to report from the events exchange queue.
  def reportableEvents: Stream[IO, AutoAckMessage[ReportableEvent[JsonObject]]] =
    RabbitUtils
      .createConsumer[ReportableEvent[JsonObject]](
        rabbit,
        conf.queueName(eventsExchangeName)
      )

  // Insert reportable events in database and publish next pending event.
  def reportEventPipe: Pipe[IO, AutoAckMessage[ReportableEvent[JsonObject]], Unit] =
    _.evalMap { autoAckMessage =>
      autoAckMessage.unwrap { event =>
        implicit val lc: LamaLogContext =
          LamaLogContext().withAccount(event.account).withFollowUpId(event.syncId)
        Queries.insertSyncEvent(event).transact(db).void *>
          publisher.dequeue(event.account.id) *>
          log.info(show"Reported event: $event")
      }
    }

  // Fetch triggerable events from database.
  def triggerableEvents: Stream[IO, TriggerableEvent[JsonObject]] =
    Queries
      .fetchTriggerableEvents(conf.coinFamily, conf.coin)
      .transact(db)

  // From triggerable events, construct next events then insert.
  def triggerEventsPipe: Pipe[IO, TriggerableEvent[JsonObject], Unit] =
    _.evalMap { event =>
      implicit val lc: LamaLogContext =
        LamaLogContext().withAccount(event.account).withFollowUpId(event.syncId)
      Queries.insertSyncEvent(event.nextWorkable).transact(db).void *>
        log.info(s"Next event: ${event.nextWorkable.asJson.toString}")
    }

}
