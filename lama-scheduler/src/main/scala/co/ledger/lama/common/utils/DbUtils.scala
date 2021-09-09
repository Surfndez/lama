package co.ledger.lama.common.utils

import cats.effect.IO
import org.flywaydb.core.Flyway

object DbUtils {

  def flywayMigrate(conf: PostgresConfig): IO[Unit] =
    IO(flyway(conf).migrate()).void

  def flyway(conf: PostgresConfig) =
    Flyway
      .configure()
      .dataSource(conf.url, conf.user, conf.password)
      .locations("classpath:/db/migration/")
      .load()

}
