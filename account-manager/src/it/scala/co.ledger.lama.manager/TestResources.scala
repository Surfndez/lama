package co.ledger.lama.manager

import cats.effect.{IO, Resource}
import cats.implicits._
import co.ledger.lama.common.models.{Account, AccountGroup, Coin, CoinFamily}
import co.ledger.lama.common.utils.{DbUtils, ResourceUtils}
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import co.ledger.lama.manager.config.Config
import co.ledger.lama.manager.utils.RedisUtils
import com.redis.RedisClient
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeType
import doobie.util.transactor.Transactor
import org.flywaydb.core.Flyway
import pureconfig.ConfigSource

trait TestResources {

  val conf: Config = ConfigSource.default.loadOrThrow[Config]

  val accountTest: Account =
    Account("12345", CoinFamily.Bitcoin, Coin.Btc, AccountGroup("TestGroup"))

  val transactor: Resource[IO, Transactor[IO]] = ResourceUtils.postgresTransactor(conf.postgres)

  val rabbit: Resource[IO, RabbitClient[IO]] = RabbitUtils.createClient(conf.rabbit)

  val redis: Resource[IO, RedisClient] = RedisUtils.createClient(conf.redis)

  def appResources: Resource[IO, (Transactor[IO], RedisClient, RabbitClient[IO])] =
    for {
      db           <- transactor
      redisClient  <- redis
      rabbitClient <- rabbit
    } yield (db, redisClient, rabbitClient)

  val flyway: Flyway = DbUtils.flyway(conf.postgres)

  private def cleanDb(): IO[Unit] =
    IO(flyway.clean()) *> IO(flyway.migrate())

  private def cleanRedis(): IO[Unit] =
    redis.use { client =>
      IO(
        client.del(
          Publisher.onGoingEventsCounterKey(accountTest.id),
          Publisher.pendingEventsKey(accountTest.id)
        )
      ).void
    }

  private def cleanRabbit(): IO[Unit] =
    rabbit.use { client =>
      val coinConfs                = conf.orchestrator.coins
      val lamaEventsExchangeName   = conf.orchestrator.lamaEventsExchangeName
      val workerEventsExchangeName = conf.orchestrator.workerEventsExchangeName

      val deleteQueues =
        coinConfs.map { coinConf =>
          RabbitUtils.deleteBindings(
            client,
            List(
              coinConf.queueName(workerEventsExchangeName),
              coinConf.queueName(lamaEventsExchangeName)
            )
          )
        }.sequence

      val deleteExchanges =
        RabbitUtils.deleteExchanges(client, List(workerEventsExchangeName, lamaEventsExchangeName))

      val exchanges = List(
        (workerEventsExchangeName, ExchangeType.Topic),
        (lamaEventsExchangeName, ExchangeType.Topic)
      )

      val bindings = coinConfs
        .flatMap { coinConf =>
          List(
            (
              lamaEventsExchangeName,
              coinConf.routingKey,
              coinConf.queueName(lamaEventsExchangeName)
            ),
            (
              workerEventsExchangeName,
              coinConf.routingKey,
              coinConf.queueName(workerEventsExchangeName)
            )
          )
        }

      deleteQueues *>
        deleteExchanges *>
        RabbitUtils.declareExchanges(client, exchanges) *>
        RabbitUtils.declareBindings(client, bindings)
    }

  def setup(): IO[Unit] = cleanDb() &> cleanRedis() &> cleanRabbit()

}
