package co.ledger.lama.scheduler.domain.services

import cats.effect.{Concurrent, ContextShift, IO, Timer}
import co.ledger.lama.common.utils.rabbitmq.AutoAckMessage
import co.ledger.lama.scheduler.config.config.{CoinConfig, OrchestratorConfig}
import co.ledger.lama.scheduler.domain.models.{ReportableEvent, WorkableEvent}
import doobie.util.transactor.Transactor
import fs2.Stream
import io.circe.JsonObject

import java.util.UUID
import scala.concurrent.duration._

trait Orchestrator {

  val tasks: List[SyncEventTask]

  // duration to awake every 'd' candidates stream
  val awakeEvery: FiniteDuration = 5.seconds

  def run(
      stopAtNbTick: Option[Long] = None
  )(implicit c: Concurrent[IO], t: Timer[IO]): Stream[IO, Unit] =
    Stream
      .emits(tasks)
      .map { task =>
        val publishPipeline = task.publishWorkerEvents(awakeEvery, stopAtNbTick)
        val reportPipeline  = task.reportEvents
        val triggerPipeline = task.trigger(awakeEvery)

        // Race all inner streams simultaneously.
        publishPipeline
          .concurrently(reportPipeline)
          .concurrently(triggerPipeline)
      }
      .parJoinUnbounded

}

class CoinOrchestrator(
    val conf: OrchestratorConfig,
    val db: Transactor[IO],
    val mkEventStream: CoinConfig => Stream[IO, AutoAckMessage[ReportableEvent[JsonObject]]],
    val mkNotifier: CoinConfig => Notifier,
    val mkPublishingQueue: (
      WorkableEvent[JsonObject] => IO[Unit]
      ) => PublishingQueue[UUID, WorkableEvent[JsonObject]]
)(implicit cs: ContextShift[IO])
    extends Orchestrator {

  val tasks: List[CoinSyncEventTask] =
    conf.coins
      .map { coinConf =>
        new CoinSyncEventTask(
          coinConf,
          db,
          mkEventStream,
          mkNotifier,
          mkPublishingQueue
        )
      }

}
