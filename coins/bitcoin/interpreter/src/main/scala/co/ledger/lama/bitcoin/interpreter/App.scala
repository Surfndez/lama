package co.ledger.lama.bitcoin.interpreter

import cats.effect.{IO, IOApp, Resource}
import cats.implicits._
import co.ledger.lama.common.services.RabbitNotificationService
import co.ledger.lama.common.services.grpc.HealthService
import co.ledger.lama.common.utils.ResourceUtils.{grpcServer, postgresTransactor}
import co.ledger.lama.common.utils.DbUtils
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import pureconfig.ConfigSource

object App extends IOApp.Simple {

  def run: IO[Unit] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val resources = for {
      rabbit <- RabbitUtils.createClient(conf.rabbit)

      channel <- rabbit.createConnectionChannel

      publisher <- Resource.eval(
        RabbitNotificationService.publisher(
          conf.lamaNotificationsExchangeName,
          RabbitNotificationService.routingKey
        )(rabbit, channel)
      )

      // create the db transactor
      db <- postgresTransactor(conf.db.postgres)

      // define rpc service definitions
      serviceDefinitions <-
        List(
          new InterpreterGrpcService(
            new Interpreter(publisher, db, conf.maxConcurrent, conf.db.batchConcurrency)
          ).definition,
          new HealthService().definition
        ).sequence

      // create the grpc server
      grpcServer <- grpcServer(conf.grpcServer, serviceDefinitions)
    } yield grpcServer

    resources
      .use { server =>
        // migrate db then start server
        DbUtils.flywayMigrate(conf.db.postgres) *>
          IO(server.start()) *>
          IO.never
      }
  }

}
