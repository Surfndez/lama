package co.ledger.lama.scheduler

import java.util.UUID

import cats.effect.{Blocker, ContextShift, IO, Resource}
import co.ledger.lama.common.models._
import co.ledger.lama.common.utils.{DbUtils, IOAssertion, PostgresConfig}
import co.ledger.lama.scheduler.Exceptions.AccountNotFoundException
import co.ledger.lama.scheduler.config.CoinConfig
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
  val testGroup  = AccountGroup("AccountManagerSpec:54")
  val coinFamily = CoinFamily.Bitcoin
  val coin       = Coin.Btc

  val updatedSyncFrequency: Long          = 10000L
  val alternateUpdatedSyncFrequency: Long = 99999L

  val accountIdentifier: Account =
    Account(testKey, coinFamily, coin, testGroup)

  it should "register a new account" in IOAssertion {
    transactor.use { db =>
      val service = new AccountManager(db, conf.coins)
      val account = Account(testKey, coinFamily, coin, testGroup)

      for {
        response <- service.registerAccount(account, None, None)
        event    <- getLastEvent(service, response.accountId)
      } yield {
        registeredAccountId = response.accountId
        registeredSyncId = response.syncId

        // it should be an account uuid from extendKey, coinFamily, coin
        response.accountId shouldBe
          Account(
            testKey,
            CoinFamily.Bitcoin,
            Coin.Btc,
            testGroup
          ).id

        // check event
        event.map(_.account.id) shouldBe Some(response.accountId)
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

  it should "prevent the registration of an already existing account in the same group" in {
    an[PSQLException] should be thrownBy IOAssertion {
      transactor.use { db =>
        val service = new AccountManager(db, conf.coins)
        val account = Account(testKey, coinFamily, coin, testGroup)

        service
          .registerAccount(
            account,
            Some(updatedSyncFrequency),
            None
          )
      }
    }
  }

  it should "allow the registration of an already existing account in a different group" in IOAssertion {
    transactor.use { db =>
      val service  = new AccountManager(db, conf.coins)
      val newGroup = AccountGroup("AccountManagerSpec:129")
      val account  = Account(testKey, coinFamily, coin, newGroup)

      for {
        response <- service.registerAccount(
          account,
          Some(alternateUpdatedSyncFrequency),
          None
        )
      } yield {

        // it should be an account uuid from extendKey, coinFamily, coin
        response.accountId shouldBe
          Account(
            testKey,
            CoinFamily.Bitcoin,
            Coin.Btc,
            newGroup
          ).id
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
        unregisteredEvent.account.id shouldBe response.accountId
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
          response.account.identifier shouldBe testKey
          response.syncFrequency shouldBe updatedSyncFrequency
          response.lastSyncEvent shouldBe Some(unregisteredEvent)
        }
    }
  }

  it should "fail to get info from an unknown account" in {
    an[AccountNotFoundException] should be thrownBy IOAssertion {
      transactor.use { db =>
        new AccountManager(db, conf.coins)
          .getAccountInfo(
            UUID.randomUUID
          )
      }
    }
  }

  it should "get all accounts only in target group" in IOAssertion {
    transactor.use { db =>
      val service     = new AccountManager(db, conf.coins)
      val group1      = AccountGroup("First Group")
      val group2      = AccountGroup("Second Group")
      val keyA        = UUID.randomUUID().toString
      val keyB        = UUID.randomUUID().toString
      val keyC        = UUID.randomUUID().toString
      val accountIdA1 = Account(keyA, coinFamily, coin, group1).id
      val accountIdB1 = Account(keyB, coinFamily, coin, group1).id
      val accountIdA2 = Account(keyA, coinFamily, coin, group2).id
      val accountIdC2 = Account(keyC, coinFamily, coin, group2).id

      for {
        _ <- service.registerAccount(Account(keyA, coinFamily, coin, group1), None, None)
        _ <- service.registerAccount(Account(keyB, coinFamily, coin, group1), None, None)
        _ <- service.registerAccount(Account(keyA, coinFamily, coin, group2), None, None)
        _ <- service.registerAccount(Account(keyC, coinFamily, coin, group2), None, None)
        listGroup1 <- service
          .getAccounts(Some(group1), 0, 0)
          .map(_.accounts)
          .map(innerList => innerList.map(_.account.id))
        listGroup2 <- service
          .getAccounts(Some(group2), 0, 0)
          .map(_.accounts)
          .map(innerList => innerList.map(_.account.id))
        listAllGroups <- service
          .getAccounts(None, 0, 0)
          .map(_.accounts)
          .map(innerList => innerList.map(_.account.id))
      } yield {
        listGroup1 should have size 2
        listGroup1 should contain(accountIdA1)
        listGroup1 should contain(accountIdB1)

        listGroup2 should have size 2
        listGroup2 should contain(accountIdA2)
        listGroup2 should contain(accountIdC2)

        // We do not check the size on All Groups because
        // the test did register other accounts earlier
        listAllGroups should contain(accountIdA1)
        listAllGroups should contain(accountIdB1)
        listAllGroups should contain(accountIdA2)
        listAllGroups should contain(accountIdC2)
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
