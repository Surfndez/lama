package co.ledger.lama.bitcoin.transactor

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import co.ledger.lama.bitcoin.common.clients.grpc.{InterpreterGrpcClient, KeychainGrpcClient}
import co.ledger.lama.bitcoin.common.clients.http.ExplorerHttpClient
import co.ledger.lama.bitcoin.transactor.clients.grpc.BitcoinLibGrpcClient
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.common.services.Clients
import co.ledger.lama.common.services.grpc.HealthService
import co.ledger.lama.common.utils.ResourceUtils
import co.ledger.lama.common.utils.ResourceUtils.grpcClientResource
import fs2._
import pureconfig.ConfigSource

object App extends IOApp with DefaultContextLogging {

  def run(args: List[String]): IO[ExitCode] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val resources = for {

      interpreterGrpcRes <- grpcClientResource(conf.interpreter)
      keychainGrpcRes    <- grpcClientResource(conf.keychain)
      bitcoinLibGrpcRes  <- grpcClientResource(conf.bitcoinLib)
      httpClient         <- Clients.htt4s

      interpreterClient = new InterpreterGrpcClient(interpreterGrpcRes)
      keychainClient    = new KeychainGrpcClient(keychainGrpcRes)
      explorerClient    = new ExplorerHttpClient(httpClient, conf.explorer, _)
      bitcoinLibClient  = new BitcoinLibGrpcClient(bitcoinLibGrpcRes)

      serviceDefinitions <-
        List(
          new TransactorGrpcService(
            new Transactor(
              bitcoinLibClient,
              explorerClient,
              keychainClient,
              interpreterClient,
              conf.transactor
            )
          ).definition,
          new HealthService().definition
        ).sequence

      grcpService <- ResourceUtils.grpcServer(conf.grpcServer, serviceDefinitions)

    } yield grcpService

    Stream
      .resource(resources)
      .evalMap(server => IO(server.start()) *> log.info("Transactor started"))
      .evalMap(_ => IO.never)
      .compile
      .drain
      .as(ExitCode.Success)
  }

}
