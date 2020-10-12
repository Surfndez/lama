package co.ledger.lama.bitcoin.worker

import cats.effect.{ExitCode, IO, IOApp}
import co.ledger.lama.bitcoin.interpreter.protobuf.BitcoinInterpreterServiceFs2Grpc
import co.ledger.lama.bitcoin.worker.config.Config
import co.ledger.lama.bitcoin.worker.services._
import co.ledger.lama.common.utils.RabbitUtils
import co.ledger.lama.common.utils.ResourceUtils.grpcManagedChannel
import co.ledger.protobuf.bitcoin.KeychainServiceFs2Grpc
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeType
import io.grpc.ManagedChannel
import org.http4s.client.Client
import pureconfig.ConfigSource

object App extends IOApp {

  case class WorkerResources(
      rabbitClient: RabbitClient[IO],
      httpClient: Client[IO],
      keychainChannel: ManagedChannel,
      interpreterChannel: ManagedChannel
  )

  def run(args: List[String]): IO[ExitCode] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val resources = for {
      httpClient         <- Clients.htt4s
      keychainChannel    <- grpcManagedChannel(conf.keychain)
      interpreterChannel <- grpcManagedChannel(conf.interpreter)
      rabbitClient       <- Clients.rabbit(conf.rabbit)
    } yield WorkerResources(rabbitClient, httpClient, keychainChannel, interpreterChannel)

    resources.use { res =>
      val syncEventService = new SyncEventService(
        res.rabbitClient,
        conf.queueName(conf.workerEventsExchangeName),
        conf.lamaEventsExchangeName,
        conf.routingKey
      )

      val keychainClient = KeychainServiceFs2Grpc.stub[IO](res.keychainChannel)

      val keychainService = new KeychainGrpcClientService(keychainClient)

      val explorerService = new ExplorerService(res.httpClient, conf.explorer)

      val interpreterClient =
        BitcoinInterpreterServiceFs2Grpc.stub[IO](res.interpreterChannel)

      val interpreterService = new InterpreterGrpcClientService(interpreterClient)

      val worker = new Worker(
        syncEventService,
        keychainService,
        explorerService,
        interpreterService
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

        res <- worker.run.compile.lastOrError.as(ExitCode.Success)
      } yield res
    }
  }

}