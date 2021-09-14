package co.ledger.lama.scheduler

import cats.effect.{ExitCode, IO, IOApp, Resource}
import co.ledger.lama.common.utils.DbUtils
import co.ledger.lama.common.utils.ResourceUtils.postgresTransactor
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import co.ledger.lama.scheduler.config.{Config, OrchestratorConfig}
import co.ledger.lama.scheduler.domain.LamaSchedulerModule
import co.ledger.lama.scheduler.routes.AccountController
import co.ledger.lama.scheduler.utils.RedisUtils
import com.redis.RedisClient
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeType
import doobie.util.transactor.Transactor
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.{CORS, CORSConfig}
import org.http4s.implicits.http4sKleisliResponseSyntaxOptionT
import pureconfig.ConfigSource

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object App extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val appResources = for {
      t <- makeResources(conf)
      (db, rabbitClient, redisClient) = t

      methodConfig = CORSConfig(
        anyOrigin = true,
        anyMethod = true,
        allowCredentials = false,
        maxAge = 1.day.toSeconds
      )

      httpRoutes = Router[IO](
        "accounts" -> CORS(
          AccountController.accountRoutes(),
          methodConfig
        )
      ).orNotFound

      module = new LamaSchedulerModule(
        db,
        conf.orchestrator.coins,
        conf.orchestrator,
        rabbitClient,
        redisClient
      )
    } yield (rabbitClient, module, httpRoutes)

    appResources
      .use { case (rabbitClient, module, httpRoutes) =>
        declareExchangesAndBindings(rabbitClient, conf.orchestrator) *>
          DbUtils.flywayMigrate(conf.postgres) *>
          module.orchestrator.run().compile.drain *>
          BlazeServerBuilder[IO](ExecutionContext.global)
            .bindHttp(conf.server.port, conf.server.host)
            .withHttpApp(httpRoutes)
            .serve
            .compile
            .drain
      }
      .as(ExitCode.Success)
  }

  private def makeResources(
      conf: Config
  ): Resource[IO, (Transactor[IO], RabbitClient[IO], RedisClient)] =
    for {
      db           <- postgresTransactor(conf.postgres)
      rabbitClient <- RabbitUtils.createClient(conf.rabbit)
      redisClient  <- RedisUtils.createClient(conf.redis)
    } yield (db, rabbitClient, redisClient)

  // Declare rabbitmq exchanges and bindings used by workers and the orchestrator.
  private def declareExchangesAndBindings(
      rabbit: RabbitClient[IO],
      conf: OrchestratorConfig
  ): IO[Unit] = {
    val workerExchangeName = conf.workerEventsExchangeName
    val eventsExchangeName = conf.lamaEventsExchangeName

    val exchanges = List(
      (workerExchangeName, ExchangeType.Topic),
      (eventsExchangeName, ExchangeType.Topic)
    )

    val bindings = conf.coins
      .flatMap { coinConf =>
        List(
          (eventsExchangeName, coinConf.routingKey, coinConf.queueName(eventsExchangeName)),
          (workerExchangeName, coinConf.routingKey, coinConf.queueName(workerExchangeName))
        )
      }

    RabbitUtils.declareExchanges(rabbit, exchanges) *>
      RabbitUtils.declareBindings(rabbit, bindings)
  }

}
