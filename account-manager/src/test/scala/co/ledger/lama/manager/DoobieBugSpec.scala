package co.ledger.lama.manager

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import co.ledger.lama.common.models.{Coin, CoinFamily}
import co.ledger.lama.common.utils.{DbUtils, IOAssertion, PostgresConfig, ResourceUtils}
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import doobie.hikari.HikariTransactor
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import doobie.implicits._

// TODO: remove this test after using a stable 1.0.0 doobie version
class DoobieBugSpec extends AnyFlatSpecLike with Matchers with BeforeAndAfterAll {
  val db: EmbeddedPostgres = EmbeddedPostgres.start()

  val dbUser     = "postgres"
  val dbPassword = ""
  val dbUrl      = db.getJdbcUrl(dbUser, "postgres")
  val pgConfig   = PostgresConfig(dbUrl, dbUser, dbPassword)

  private val migrateDB: IO[Unit] = DbUtils.flywayMigrate(pgConfig)

  val transactor: Resource[IO, HikariTransactor[IO]] =
    ResourceUtils.postgresTransactor(pgConfig)

  it should "not trigger an error: connection is not available" in IOAssertion {
    transactor.use { db =>
      Queries
        .fetchTriggerableEvents(CoinFamily.Bitcoin, Coin.Btc)
        .transact(db)
        .repeatN(100)
        .compile
        .toList
        .map { _ =>
          1 shouldBe 1
        }
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    migrateDB.unsafeRunSync()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    db.close()
  }

}
