package co.ledger.lama.bitcoin.api.routes

import buildinfo.BuildInfo
import cats.effect.{ContextShift, IO, Timer}
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.protobuf.lama.common.HealthCheckResponse.ServingStatus
import co.ledger.protobuf.lama.common.HealthFs2Grpc
import co.ledger.protobuf.lama.common._
import io.circe.{Encoder, Json}
import io.circe.syntax._
import io.grpc.Metadata
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.circe.CirceEntityCodec._
import cats.syntax.all._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import io.circe.JsonObject

object HealthController extends Http4sDsl[IO] with DefaultContextLogging {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val t: Timer[IO]         = IO.timer(ExecutionContext.global)

  case class HealthStatus(service: String, status: ServingStatus)

  case class ComponentInfo(status: ServingStatus, version: VersionResponse) {}

  implicit val statusEncoder: Encoder[ServingStatus] =
    (s: ServingStatus) => Json.fromString(s.name)

  implicit val versionEncoder: Encoder[VersionResponse] =
    (versionData: VersionResponse) =>
      Json.fromJsonObject(
        JsonObject(
          "version" -> Json.fromString(versionData.version),
          "sha1"    -> Json.fromString(versionData.sha1)
        )
      )

  implicit val componentInfoEncoder: Encoder[ComponentInfo] =
    (info: ComponentInfo) =>
      Json
        .fromJsonObject(JsonObject("status" -> info.status.asJson))
        .deepMerge(info.version.asJson)

  private def getComponentServingStatus(client: HealthFs2Grpc[IO, Metadata]): IO[ServingStatus] =
    client
      .check(new HealthCheckRequest(), new Metadata)
      .timeout(5.seconds)
      .handleErrorWith(_ => IO.pure(HealthCheckResponse(ServingStatus.NOT_SERVING)))
      .map(_.status)

  // Components might not answer getVersion correctly
  // (keychain for example uses a google-provided proto)
  // Having the correct status but no version MUST NOT be an error,
  // so the error is dealt with
  private def getComponentVersion(client: HealthFs2Grpc[IO, Metadata]): IO[VersionResponse] =
    client
      .version(new Empty(), new Metadata)
      .timeout(5.seconds)
      .handleErrorWith(_ => IO.pure(VersionResponse("n/a", "n/a")))

  private def getComponentInfo(client: HealthFs2Grpc[IO, Metadata]): IO[ComponentInfo] = {
    for {
      status  <- getComponentServingStatus(client)
      version <- getComponentVersion(client)
    } yield ComponentInfo(status, version)
  }

  def routes(
      accountManagerHealthClient: HealthFs2Grpc[IO, Metadata],
      interpreterHealthClient: HealthFs2Grpc[IO, Metadata],
      transactorHealthClient: HealthFs2Grpc[IO, Metadata],
      workerHealthClient: HealthFs2Grpc[IO, Metadata],
      keychainHealthClient: HealthFs2Grpc[IO, Metadata]
  ): HttpRoutes[IO] =
    HttpRoutes.of[IO] { case GET -> Root =>
      Map(
        "api" -> IO.pure(
          ComponentInfo(
            ServingStatus.SERVING,
            VersionResponse(
              version = BuildInfo.version,
              sha1 = BuildInfo.gitHeadCommit.getOrElse("n/a")
            )
          )
        ),
        "interpreter"     -> getComponentInfo(interpreterHealthClient),
        "account_manager" -> getComponentInfo(accountManagerHealthClient),
        "transactor"      -> getComponentInfo(transactorHealthClient),
        "worker"          -> getComponentInfo(workerHealthClient),
        "keychain"        -> getComponentInfo(keychainHealthClient)
      ).parUnorderedSequence
        .flatMap { componentInfos =>
          if (componentInfos.values.exists(_.status != ServingStatus.SERVING))
            InternalServerError(componentInfos)
          else
            Ok(componentInfos)
        }
    }
}
