package co.ledger.lama.bitcoin.interpreter

import cats.effect.{IO, Resource}
import co.ledger.lama.common.utils.{DbUtils, ResourceUtils}
import doobie.util.transactor.Transactor
import org.flywaydb.core.Flyway
import pureconfig.ConfigSource

trait TestResources {

  val conf: Config   = ConfigSource.default.loadOrThrow[Config]
  val flyway: Flyway = DbUtils.flyway(conf.db.postgres)

  def setup(): IO[Unit] = IO(flyway.clean()) *> IO(flyway.migrate())

  def appResources: Resource[IO, Transactor[IO]] =
    ResourceUtils.postgresTransactor(conf.db.postgres)

}
