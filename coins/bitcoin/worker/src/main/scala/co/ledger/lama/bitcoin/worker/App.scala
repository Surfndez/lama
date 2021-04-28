package co.ledger.lama.bitcoin.worker

import cats.effect.{ExitCode, IO, IOApp}
import co.ledger.lama.bitcoin.common.clients.grpc.{InterpreterGrpcClient, KeychainGrpcClient}
import co.ledger.lama.bitcoin.common.clients.http.ExplorerHttpClient
import co.ledger.lama.bitcoin.worker.config.Config
import co.ledger.lama.bitcoin.worker.services._
import co.ledger.lama.common.clients.grpc.GrpcClientResource
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.common.services.Clients
import co.ledger.lama.common.services.grpc.HealthService
import co.ledger.lama.common.models.Coin
import co.ledger.lama.common.utils.ResourceUtils
import co.ledger.lama.common.utils.ResourceUtils.grpcClientResource
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeType
import io.grpc.Server
import org.http4s.client.Client
import pureconfig.ConfigSource

object App extends IOApp with DefaultContextLogging {

  case class WorkerResources(
      rabbitClient: RabbitClient[IO],
      httpClient: Client[IO],
      keychainClientRes: GrpcClientResource,
      interpreterClientRes: GrpcClientResource,
      server: Server
  )

  def run(args: List[String]): IO[ExitCode] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val resources = for {
      httpClient         <- Clients.htt4s
      keychainClientRes  <- grpcClientResource(conf.keychain)
      interpreterGrpcRes <- grpcClientResource(conf.interpreter)
      rabbitClient       <- Clients.rabbit(conf.rabbit)

      healthServiceDefinition <- new HealthService().definition

      grcpService <- ResourceUtils.grpcServer(conf.grpcServer, List(healthServiceDefinition))

    } yield WorkerResources(
      rabbitClient,
      httpClient,
      keychainClientRes,
      interpreterGrpcRes,
      grcpService
    )

    resources.use { res =>
      val syncEventService = new RabbitSyncEventService(
        res.rabbitClient,
        conf.queueName(conf.workerEventsExchangeName),
        conf.lamaEventsExchangeName,
        conf.routingKey
      )

      val keychainClient    = new KeychainGrpcClient(res.keychainClientRes)
      val interpreterClient = new InterpreterGrpcClient(res.interpreterClientRes)
      val explorerClient    = new ExplorerHttpClient(res.httpClient, conf.explorer, _)

      val cursorStateService: Coin => CursorStateService[IO] =
        c => CursorStateService(explorerClient(c), interpreterClient).getLastValidState(_, _, _)

      val worker = new Worker(
        syncEventService,
        keychainClient,
        explorerClient,
        interpreterClient,
        cursorStateService
      )

      for {
        _ <- RabbitUtils.declareExchanges(
          res.rabbitClient,
          List(
            (conf.workerEventsExchangeName, ExchangeType.Topic),
            (conf.lamaEventsExchangeName, ExchangeType.Topic)
          )
        )
        _ <- RabbitUtils.declareBindings(
          res.rabbitClient,
          List(
            (
              conf.workerEventsExchangeName,
              conf.routingKey,
              conf.queueName(conf.workerEventsExchangeName)
            ),
            (
              conf.lamaEventsExchangeName,
              conf.routingKey,
              conf.queueName(conf.lamaEventsExchangeName)
            )
          )
        )

        _ <- IO(res.server.start()) *> log.info("Worker started")

        res <- worker.run.compile.lastOrError.as(ExitCode.Success)
      } yield res
    }
  }

}
