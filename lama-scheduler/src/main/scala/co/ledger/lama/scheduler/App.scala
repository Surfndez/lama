package co.ledger.lama.scheduler

import cats.effect.{ExitCode, IO, IOApp, Resource}
import co.ledger.lama.common.services.grpc.HealthService
import co.ledger.lama.common.utils.{DbUtils, GrpcServerConfig}
import co.ledger.lama.common.utils.ResourceUtils.{grpcServer, postgresTransactor}
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import co.ledger.lama.scheduler.config.{Config, OrchestratorConfig}
import co.ledger.lama.scheduler.domain.LamaSchedulerModule
import co.ledger.lama.scheduler.domain.adapters.primary.grpc.AccountManagerGrpcService
import co.ledger.lama.scheduler.utils.RedisUtils
import com.redis.RedisClient
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeType
import doobie.util.transactor.Transactor
import io.grpc.Server
import pureconfig.ConfigSource

object App extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val appResources = for {
      t <- makeResources(conf)
      (db, rabbitClient, redisClient) = t
      module = new LamaSchedulerModule(
        db,
        conf.orchestrator.coins,
        conf.orchestrator,
        rabbitClient,
        redisClient
      )
      server <- makeGrpcServer(conf.grpcServer, module)
    } yield (rabbitClient, module, server)

    appResources
      .use { case (rabbitClient, module, server) =>
        declareExchangesAndBindings(rabbitClient, conf.orchestrator) *>
          DbUtils.flywayMigrate(conf.postgres) *>
          IO(server.start()) *>
          module.orchestrator.run().compile.drain

      }
      .as(ExitCode.Success)
  }

  private def makeGrpcServer(config: GrpcServerConfig, module: LamaSchedulerModule): Resource[IO, Server] = {
    // define rpc service definitions
    val serviceDefinitions = List(
      new AccountManagerGrpcService(
        module.accountManager
      ).definition,
      new HealthService().definition
    )

    // create the grpc server
    grpcServer(config, serviceDefinitions)
  }

  private def makeResources(
      conf: Config
  ): Resource[IO, (Transactor[IO], RabbitClient[IO], RedisClient)] =
    for {
      db <- postgresTransactor(conf.postgres)
      rabbitClient <- RabbitUtils.createClient(conf.rabbit)
      redisClient <- RedisUtils.createClient(conf.redis)
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
