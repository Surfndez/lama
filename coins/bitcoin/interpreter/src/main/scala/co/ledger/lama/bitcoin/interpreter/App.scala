package co.ledger.lama.bitcoin.interpreter

import cats.effect.{ExitCode, IO, IOApp, Resource}
import co.ledger.lama.bitcoin.common.clients.http.ExplorerHttpClient
import co.ledger.lama.common.services.{Clients, RabbitNotificationService}
import co.ledger.lama.common.services.grpc.HealthService
import co.ledger.lama.common.utils.ResourceUtils.{grpcServer, postgresTransactor}
import co.ledger.lama.common.utils.DbUtils
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import fs2.Stream
import pureconfig.ConfigSource

object App extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val resources = for {

      rabbit  <- RabbitUtils.createClient(conf.rabbit)
      channel <- rabbit.createConnectionChannel

      publisher <- Resource.eval(
        RabbitNotificationService.publisher(
          conf.lamaNotificationsExchangeName,
          RabbitNotificationService.routingKey
        )(rabbit, channel)
      )

      httpClient <- Clients.htt4s
      explorerService = new ExplorerHttpClient(httpClient, conf.explorer, _)

      // create the db transactor
      db <- postgresTransactor(conf.db.postgres)

      // define rpc service definitions
      serviceDefinitions = List(
        new InterpreterGrpcService(
          new Interpreter(
            publisher,
            explorerService,
            db,
            conf.maxConcurrent,
            conf.db.batchConcurrency
          )
        ).definition,
        new HealthService().definition
      )

      // create the grpc server
      grpcServer <- grpcServer(conf.grpcServer, serviceDefinitions)
    } yield grpcServer

    Stream
      .resource(resources)
      .evalMap { server =>
        // migrate db then start server
        DbUtils.flywayMigrate(conf.db.postgres) *> IO(server.start())
      }
      .evalMap(_ => IO.never)
      .compile
      .drain
      .as(ExitCode.Success)
  }

}
