package co.ledger.lama.manager

import cats.effect.{IO, IOApp}
import cats.implicits._
import co.ledger.lama.common.services.grpc.HealthService
import co.ledger.lama.common.utils.DbUtils
import co.ledger.lama.common.utils.ResourceUtils.{grpcServer, postgresTransactor}
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import co.ledger.lama.manager.config.{Config, OrchestratorConfig}
import co.ledger.lama.manager.utils.RedisUtils
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeType
import pureconfig.ConfigSource

object App extends IOApp.Simple {

  def run: IO[Unit] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val resources = for {

      // create the db transactor
      db <- postgresTransactor(conf.postgres)

      // rabbitmq client
      rabbitClient <- RabbitUtils.createClient(conf.rabbit)

      // redis client
      redisClient <- RedisUtils.createClient(conf.redis)

      accountManager = new AccountManager(db, conf.orchestrator.coins)

      // define rpc service definitions
      serviceDefinitions <-
        List(
          new AccountManagerGrpcService(
            accountManager
          ).definition,
          new HealthService().definition
        ).sequence

      // create the grpc server
      grpcServer <- grpcServer(conf.grpcServer, serviceDefinitions)

    } yield (db, rabbitClient, redisClient, grpcServer)

    // start the grpc server and run the orchestrator stream
    resources
      .use { case (db, rabbitClient, redisClient, server) =>
        val orchestrator = new CoinOrchestrator(
          conf.orchestrator,
          db,
          rabbitClient,
          redisClient
        )

        declareExchangesAndBindings(rabbitClient, conf.orchestrator) *>
          DbUtils.flywayMigrate(conf.postgres) *>
          IO(server.start()) *>
          orchestrator.run().compile.lastOrError
      }
  }

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
