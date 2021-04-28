package co.ledger.lama.common.services.grpc

import buildinfo.BuildInfo
import cats.effect.{IO, Resource}
import co.ledger.protobuf.lama.common.HealthCheckResponse._
import co.ledger.protobuf.lama.common._
import io.grpc.{Metadata, ServerServiceDefinition}

class HealthService extends HealthFs2Grpc[IO, Metadata] {
  def definition: Resource[IO, ServerServiceDefinition] =
    HealthFs2Grpc.bindServiceResource(this)

  def check(request: HealthCheckRequest, ctx: Metadata): IO[HealthCheckResponse] =
    IO.pure(HealthCheckResponse(ServingStatus.SERVING))

  def watch(request: HealthCheckRequest, ctx: Metadata): fs2.Stream[IO, HealthCheckResponse] =
    fs2.Stream(HealthCheckResponse(ServingStatus.SERVING))

  def version(request: Empty, ctx: Metadata): IO[VersionResponse] =
    IO.pure(VersionResponse(BuildInfo.version, BuildInfo.gitHeadCommit.getOrElse("n/a")))
}
