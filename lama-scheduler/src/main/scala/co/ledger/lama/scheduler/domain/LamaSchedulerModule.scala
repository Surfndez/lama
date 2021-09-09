package co.ledger.lama.scheduler.domain

import cats.effect.{ContextShift, IO}
import co.ledger.lama.scheduler.config.{CoinConfig, OrchestratorConfig}
import co.ledger.lama.scheduler.domain.services.{AccountManager, CoinOrchestrator, Orchestrator}
import com.redis.RedisClient
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import doobie.util.transactor.Transactor

final class LamaSchedulerModule(
    db: Transactor[IO],
    coinConfigs: List[CoinConfig],
    orchestratorConfig: OrchestratorConfig,
    rabbitClient: RabbitClient[IO],
    reddisClient: RedisClient
)(implicit cs: ContextShift[IO]) {

  lazy val orchestrator: Orchestrator =
    new CoinOrchestrator(orchestratorConfig, db, rabbitClient, reddisClient)

  lazy val accountManager: AccountManager = new AccountManager(db, coinConfigs)
}
