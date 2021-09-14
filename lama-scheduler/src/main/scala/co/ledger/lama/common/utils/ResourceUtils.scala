package co.ledger.lama.common.utils

import cats.effect.{Async, Blocker, ContextShift, IO, Resource, Timer}
import co.ledger.lama.common.logging.DefaultContextLogging
import com.zaxxer.hikari.HikariConfig
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import fs2.Stream

object ResourceUtils extends DefaultContextLogging {

  def retriableResource[F[_], O](
      label: String,
      resource: Resource[F, O],
      policy: RetryPolicy = RetryPolicy.linear()
  )(implicit
      T: Timer[F],
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
  )(implicit contextShift: ContextShift[IO], timer: Timer[IO]): Resource[IO, HikariTransactor[IO]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](conf.poolSize)

      te <- ExecutionContexts.cachedThreadPool[IO]

      _ = log.logger.info("Creating postgres client")

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
          ce,                              // await connection here
          Blocker.liftExecutionContext(te) // execute JDBC operations here
        )
      )

      _ = log.logger.info("Postgres client created")
    } yield db

}
