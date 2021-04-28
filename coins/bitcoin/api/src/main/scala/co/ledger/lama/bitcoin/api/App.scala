package co.ledger.lama.bitcoin.api

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import co.ledger.lama.bitcoin.api.Config.Config
import co.ledger.lama.bitcoin.api.middlewares.LoggingMiddleware._
import co.ledger.lama.bitcoin.api.routes.{AccountController, HealthController}
import co.ledger.lama.bitcoin.common.clients.grpc.{
  InterpreterGrpcClient,
  KeychainGrpcClient,
  TransactorGrpcClient
}
import co.ledger.lama.common.clients.grpc.{AccountManagerGrpcClient, GrpcClientResource}
import co.ledger.lama.common.utils.ResourceUtils.grpcClientResource
import co.ledger.protobuf.lama.common.HealthFs2Grpc
import io.grpc.Metadata
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware._
import org.http4s.implicits._
import pureconfig.ConfigSource

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object App extends IOApp {

  case class ClientResources(
      accountManagerGrpcRes: GrpcClientResource,
      interpreterGrpcRes: GrpcClientResource,
      transactorGrpcRes: GrpcClientResource,
      workerGrpcRes: GrpcClientResource,
      keychainGrpcRes: GrpcClientResource
  )

  def run(args: List[String]): IO[ExitCode] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val resources = for {
      accountManagerGrpcChannel <- grpcClientResource(conf.accountManager)
      interpreterGrpcChannel    <- grpcClientResource(conf.bitcoin.interpreter)
      transactorGrpcChannel     <- grpcClientResource(conf.bitcoin.transactor)
      workerGrpcChannel         <- grpcClientResource(conf.bitcoin.worker)
      keychainGrpcChannel       <- grpcClientResource(conf.bitcoin.keychain)
    } yield ClientResources(
      accountManagerGrpcRes = accountManagerGrpcChannel,
      interpreterGrpcRes = interpreterGrpcChannel,
      transactorGrpcRes = transactorGrpcChannel,
      workerGrpcRes = workerGrpcChannel,
      keychainGrpcRes = keychainGrpcChannel
    )

    resources.use { res =>
      val methodConfig = CORSConfig(
        anyOrigin = true,
        anyMethod = true,
        allowCredentials = false,
        maxAge = 1.day.toSeconds
      )

      val accountManager = new AccountManagerGrpcClient(res.accountManagerGrpcRes)
      val keychainClient = new KeychainGrpcClient(res.keychainGrpcRes)

      val httpRoutes = Router[IO](
        "accounts" -> CORS(
          loggingMiddleWare(
            AccountController
              .routes(
                keychainClient,
                accountManager,
                new InterpreterGrpcClient(res.interpreterGrpcRes)
              ) <+> AccountController
              .transactionsRoutes(
                keychainClient,
                accountManager,
                new TransactorGrpcClient(res.transactorGrpcRes)
              )
          ),
          methodConfig
        ),
        "_health" -> CORS(
          HealthController.routes(
            getHealthFs2GrpcStub(res.accountManagerGrpcRes),
            getHealthFs2GrpcStub(res.interpreterGrpcRes),
            getHealthFs2GrpcStub(res.transactorGrpcRes),
            getHealthFs2GrpcStub(res.workerGrpcRes),
            getHealthFs2GrpcStub(res.keychainGrpcRes)
          ),
          methodConfig
        )
      ).orNotFound

      BlazeServerBuilder[IO](ExecutionContext.global)
        .bindHttp(conf.server.port, conf.server.host)
        .withHttpApp(httpRoutes)
        .serve
        .compile
        .drain
        .as(ExitCode.Success)
    }
  }

  private def getHealthFs2GrpcStub(
      clientResource: GrpcClientResource
  ): HealthFs2Grpc[IO, Metadata] =
    HealthFs2Grpc.stub[IO](clientResource.dispatcher, clientResource.channel)

}
