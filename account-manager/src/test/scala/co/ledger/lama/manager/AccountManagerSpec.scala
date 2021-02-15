package co.ledger.lama.manager

import java.util.UUID

import cats.effect.{Blocker, ContextShift, IO, Resource}
import co.ledger.lama.common.models._
import co.ledger.lama.common.utils.{DbUtils, IOAssertion, PostgresConfig}
import co.ledger.lama.manager.Exceptions.AccountNotFoundException
import co.ledger.lama.manager.config.CoinConfig
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import io.circe.JsonObject
import org.postgresql.util.PSQLException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.concurrent.ExecutionContext

class AccountManagerSpec extends AnyFlatSpecLike with Matchers with BeforeAndAfterAll {

  val db: EmbeddedPostgres =
    EmbeddedPostgres.start()

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val conf: TestServiceConfig = ConfigSource.default.loadOrThrow[TestServiceConfig]

  val dbUser     = "postgres"
  val dbPassword = ""
  val dbUrl      = db.getJdbcUrl(dbUser, "postgres")

  val transactor: Resource[IO, HikariTransactor[IO]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
      te <- ExecutionContexts.cachedThreadPool[IO] // our transaction EC
      xa <- HikariTransactor.newHikariTransactor[IO](
        "org.postgresql.Driver",         // driver classname
        dbUrl,                           // connect URL
        dbUser,                          // username
        dbPassword,                      // password
        ce,                              // await connection here
        Blocker.liftExecutionContext(te) // execute JDBC operations here
      )
    } yield xa

  var registeredAccountId: UUID = _
  var registeredSyncId: UUID    = _

  val testKey    = "12345"
  val coinFamily = CoinFamily.Bitcoin
  val coin       = Coin.Btc

  val updatedSyncFrequency: Long = 10000L

  val accountIdentifier: AccountIdentifier =
    AccountIdentifier(testKey, CoinFamily.Bitcoin, Coin.Btc)

  it should "register a new account" in IOAssertion {
    transactor.use { db =>
      val service = new AccountManager(db, conf.coins)

      for {
        response <- service.registerAccount(testKey, coinFamily, coin, None, None)
        event    <- getLastEvent(service, response.accountId)
      } yield {
        registeredAccountId = response.accountId
        registeredSyncId = response.syncId

        // it should be an account uuid from extendKey, coinFamily, coin
        response.accountId shouldBe
          AccountIdentifier(
            testKey,
            CoinFamily.Bitcoin,
            Coin.Btc
          ).id

        // check event
        event.map(_.accountId) shouldBe Some(response.accountId)
        event.map(_.syncId) shouldBe Some(response.syncId)
        event.map(_.status) shouldBe Some(Status.Registered)
        event.flatMap(_.cursor) shouldBe None
        event.flatMap(_.error) shouldBe None
      }
    }
  }

  it should "update a registered account" in IOAssertion {
    transactor.use { db =>
      val service    = new AccountManager(db, conf.coins)
      val fAccountId = registeredAccountId
      val labelO     = Some("New Account")
      for {
        _              <- service.updateAccount(fAccountId, labelO, Some(updatedSyncFrequency))
        newAccountInfo <- service.getAccountInfo(fAccountId)
      } yield {
        newAccountInfo.syncFrequency shouldBe updatedSyncFrequency
        newAccountInfo.label shouldBe labelO
      }
    }
  }

  it should "not allow the registration of an already existing account" in {
    an[PSQLException] should be thrownBy IOAssertion {
      transactor.use { db =>
        val service = new AccountManager(db, conf.coins)

        service
          .registerAccount(
            testKey,
            coinFamily,
            coin,
            Some(updatedSyncFrequency),
            None
          )
      }
    }
  }

  var unregisteredSyncId: UUID                 = _
  var unregisteredEvent: SyncEvent[JsonObject] = _

  it should "unregister an account" in IOAssertion {
    transactor.use { db =>
      val service = new AccountManager(db, conf.coins)

      for {
        response <- service.unregisterAccount(registeredAccountId)
        event    <- getLastEvent(service, response.accountId)
      } yield {
        response.accountId shouldBe registeredAccountId
        unregisteredSyncId = response.syncId
        unregisteredSyncId should not be registeredSyncId

        // check event
        unregisteredEvent = event.get
        unregisteredEvent.accountId shouldBe response.accountId
        unregisteredEvent.syncId shouldBe response.syncId
        unregisteredEvent.status shouldBe Status.Unregistered
        unregisteredEvent.cursor shouldBe None
        unregisteredEvent.error shouldBe None
      }
    }
  }

  it should "return the same response if already unregistered" in IOAssertion {
    transactor.use { db =>
      val service = new AccountManager(db, conf.coins)
      for {
        response <- service.unregisterAccount(registeredAccountId)
        event    <- getLastEvent(service, response.accountId)

      } yield {
        response.accountId shouldBe registeredAccountId
        response.syncId shouldBe unregisteredSyncId
        event shouldBe Some(unregisteredEvent)
      }
    }
  }

  it should "succeed to get info from an existing account" in IOAssertion {
    transactor.use { db =>
      new AccountManager(db, conf.coins)
        .getAccountInfo(registeredAccountId)
        .map { response =>
          response.key shouldBe testKey
          response.syncFrequency shouldBe updatedSyncFrequency
          response.lastSyncEvent shouldBe Some(unregisteredEvent)
        }
    }
  }

  it should "failed to get info from an unknown account" in {
    an[AccountNotFoundException] should be thrownBy IOAssertion {
      transactor.use { db =>
        new AccountManager(db, conf.coins)
          .getAccountInfo(
            UUID.randomUUID
          )
      }
    }
  }

  private def getLastEvent(
      service: AccountManager,
      accountId: UUID
  ): IO[Option[SyncEvent[JsonObject]]] =
    service
      .getAccountInfo(accountId)
      .map(_.lastSyncEvent)

  private val migrateDB: IO[Unit] = DbUtils.flywayMigrate(PostgresConfig(dbUrl, dbUrl, dbPassword))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    migrateDB.unsafeRunSync()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    db.close()
  }

}

case class TestServiceConfig(coins: List[CoinConfig])
