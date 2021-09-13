package co.ledger.lama.scheduler.domain

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits.toFunctorOps
import co.ledger.lama.common.utils.rabbitmq.AutoAckMessage
import co.ledger.lama.scheduler.config.config.{CoinConfig, OrchestratorConfig}
import co.ledger.lama.scheduler.domain.models.{ReportableEvent, WorkableEvent}
import co.ledger.lama.scheduler.domain.services.{
  AccountManager,
  CoinOrchestrator,
  Notifier,
  Orchestrator,
  PublishingQueue
}
import doobie.util.transactor.Transactor
import fs2.Stream
import io.circe.JsonObject

import java.util.UUID

final class LamaSchedulerModule private (
    db: Transactor[IO],
    orchestratorConfig: OrchestratorConfig,
    mkEventStream: CoinConfig => Stream[IO, AutoAckMessage[ReportableEvent[JsonObject]]],
    mkNotifier: CoinConfig => Notifier,
    mkPublishingQueue: (
        WorkableEvent[JsonObject] => IO[Unit]
    ) => PublishingQueue[UUID, WorkableEvent[JsonObject]]
)(implicit cs: ContextShift[IO]) {

  lazy val orchestrator: Orchestrator =
    new CoinOrchestrator(orchestratorConfig, db, mkEventStream, mkNotifier, mkPublishingQueue)

  lazy val accountManager: AccountManager = new AccountManager(db, orchestratorConfig.coins)
}

object LamaSchedulerModule {
  def apply(
      db: Transactor[IO],
      orchestratorConfig: OrchestratorConfig,
      mkEventStream: CoinConfig => Stream[IO, AutoAckMessage[ReportableEvent[JsonObject]]],
      mkNotifier: CoinConfig => Notifier,
      mkPublishingQueue: (
          WorkableEvent[JsonObject] => IO[Unit]
      ) => PublishingQueue[UUID, WorkableEvent[JsonObject]]
  )(implicit cs: ContextShift[IO], t: Timer[IO]): Resource[IO, LamaSchedulerModule] = {
    val module =
      new LamaSchedulerModule(db, orchestratorConfig, mkEventStream, mkNotifier, mkPublishingQueue)

    Resource.make(module.orchestrator.run().compile.drain.start)(fiber => fiber.cancel).as(module)
  }
}
