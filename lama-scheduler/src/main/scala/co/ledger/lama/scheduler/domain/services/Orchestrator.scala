package co.ledger.lama.scheduler.domain.services

import cats.effect.{Concurrent, ContextShift, IO, Timer}
import co.ledger.lama.scheduler.config.OrchestratorConfig
import com.redis.RedisClient
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import doobie.util.transactor.Transactor
import fs2.Stream

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
    val rabbit: RabbitClient[IO],
    val redis: RedisClient
)(implicit cs: ContextShift[IO])
    extends Orchestrator {

  val tasks: List[CoinSyncEventTask] =
    conf.coins
      .map { coinConf =>
        new CoinSyncEventTask(
          conf.workerEventsExchangeName,
          conf.lamaEventsExchangeName,
          coinConf,
          db,
          rabbit,
          redis
        )
      }

}
