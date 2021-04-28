package co.ledger.lama.common.utils

import cats.effect.std.Dispatcher
import cats.effect.{Async, IO, Resource}
import co.ledger.lama.common.clients.grpc.GrpcClientResource
import co.ledger.lama.common.logging.DefaultContextLogging
import com.zaxxer.hikari.HikariConfig
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import fs2.Stream
import io.grpc._
import fs2.grpc.syntax.all._

import java.util.concurrent.TimeUnit
import scala.concurrent.blocking

object ResourceUtils extends DefaultContextLogging {

  def retriableResource[F[_], O](
      label: String,
      resource: Resource[F, O],
      policy: RetryPolicy = RetryPolicy.linear()
  )(implicit
      F: Async[F]
  ): Resource[F, O] =
    Stream
      .resource(resource)
      .attempts(policy)
      .evalTap {
        case Left(value) =>
          F.delay(log.logger.info(s"$label - resource acquisition failed : ${value.getMessage}"))
        case Right(_) => F.unit
      }
      .collectFirst { case Right(res) =>
        res
      }
      .compile
      .resource
      .lastOrError

  def postgresTransactor(
      conf: PostgresConfig
  ): Resource[IO, HikariTransactor[IO]] =
    for {

      ce <- ExecutionContexts.fixedThreadPool[IO](conf.poolSize)

      _ <- Resource.eval(log.info("Creating postgres client"))

      hikariConf = {
        val hc = new HikariConfig()
        hc.setDriverClassName(conf.driver) // driver classname
        hc.setJdbcUrl(conf.url)            // connect URL
        hc.setUsername(conf.user)          // username
        hc.setPassword(conf.password)      // password
        hc.setAutoCommit(false)            // doobie uses `.transact(db)` for commit
        hc
      }

      db <- retriableResource(
        "Create postgres client",
        HikariTransactor.fromHikariConfig[IO](
          hikariConf,
          ce
        )
      )

      _ <- Resource.eval(log.info("Postgres client created"))
    } yield db

  def grpcServer(
      conf: GrpcServerConfig,
      services: List[ServerServiceDefinition]
  ): Resource[IO, Server] =
    Resource.make {
      IO.delay {
        services
          .foldLeft(ServerBuilder.forPort(conf.port)) { case (builder, service) =>
            builder.addService(service)
          }
          .build()
      }
    } { server =>
      IO.delay {
        server.shutdown()
        if (!blocking(server.awaitTermination(30, TimeUnit.SECONDS))) {
          server.shutdownNow()
          ()
        }
      }
    }

  def grpcClientResource(conf: GrpcClientConfig): Resource[IO, GrpcClientResource] =
    for {
      dispatcher <- Dispatcher[IO]
      channel <-
        if (conf.ssl) {
          ManagedChannelBuilder
            .forAddress(conf.host, conf.port)
            .useTransportSecurity()
            .resource[IO]
        } else {
          ManagedChannelBuilder
            .forAddress(conf.host, conf.port)
            .usePlaintext()
            .resource[IO]
        }
    } yield GrpcClientResource(dispatcher, channel)

}
