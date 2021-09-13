package co.ledger.lama.scheduler

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import co.ledger.lama.common.utils.DbUtils
import co.ledger.lama.common.utils.ResourceUtils.postgresTransactor
import co.ledger.lama.common.utils.rabbitmq.{AutoAckMessage, RabbitUtils}
import co.ledger.lama.scheduler.config.config.{CoinConfig, Config, OrchestratorConfig}
import co.ledger.lama.scheduler.domain.LamaSchedulerModule
import co.ledger.lama.scheduler.domain.adapters.secondary.notifier.lama.RabbitNotifier
import co.ledger.lama.scheduler.domain.adapters.secondary.queue.RedisPublishingQueue
import co.ledger.lama.scheduler.domain.models.{ReportableEvent, WorkableEvent}
import co.ledger.lama.scheduler.domain.services.{Notifier, PublishingQueue}
import co.ledger.lama.scheduler.routes.AccountController
import co.ledger.lama.scheduler.utils.RedisUtils
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{ExchangeName, ExchangeType}
import doobie.util.transactor.Transactor
import fs2.Stream
import io.circe.JsonObject
import org.http4s.implicits.http4sKleisliResponseSyntaxOptionT
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.{CORS, CORSConfig}
import pureconfig.ConfigSource
import co.ledger.lama.scheduler.config.NotifierConfig
import co.ledger.lama.scheduler.domain.adapters.secondary.notifier.knative
import com.redis.RedisClient

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object App extends IOApp {

  case class ClientResources(
      db: Transactor[IO],
      mkEventStream: CoinConfig => Stream[IO, AutoAckMessage[ReportableEvent[JsonObject]]],
      mkNotifier: CoinConfig => Notifier,
      mkPublishingQueue: (
          WorkableEvent[JsonObject] => IO[Unit]
      ) => PublishingQueue[UUID, WorkableEvent[JsonObject]]
  )

  def run(args: List[String]): IO[ExitCode] = {
    val conf = ConfigSource.default.loadOrThrow[Config]

    val program = for {
      resources <- makeSecondaryAdapters(conf)
      module <- LamaSchedulerModule(
        resources.db,
        conf.orchestrator,
        resources.mkEventStream,
        resources.mkNotifier,
        resources.mkPublishingQueue
      )
      _ <- startPrimaryAdapters(conf, module)
    } yield ()

    program
      .use(_ => IO.never)
      .as(ExitCode.Success)
  }

  private def startPrimaryAdapters(
      conf: Config,
      module: LamaSchedulerModule
  ): Resource[IO, Unit] = {
    val _ = module
    val methodConfig = CORSConfig(
      anyOrigin = true,
      anyMethod = true,
      allowCredentials = false,
      maxAge = 1.day.toSeconds
    )

    val httpRoutes = Router[IO](
      "accounts" -> CORS(
        AccountController.accountRoutes(),
        methodConfig
      )
    ).orNotFound

    val httpServer = BlazeServerBuilder[IO](ExecutionContext.global)
      .bindHttp(conf.server.port, conf.server.host)
      .withHttpApp(httpRoutes)
      .serve
      .compile
      .drain

    Resource
      .make(httpServer.start)(fiber => fiber.cancel)
      .void
  }

  def makeSecondaryAdapters(conf: Config): Resource[IO, ClientResources] =
    for {
      db           <- postgresTransactor(conf.postgres)
      _            <- Resource.liftK[IO].apply(DbUtils.flywayMigrate(conf.postgres))
      rabbitClient <- RabbitUtils.createClient(conf.rabbit)
      redisClient  <- RedisUtils.createClient(conf.redis)
      mkNotifier   <- mkNotifierResource(rabbitClient, conf)
    } yield ClientResources(
      db,
      mkEventStream(rabbitClient, conf.orchestrator),
      mkNotifier,
      mkPublishingQueue(redisClient)
    )

  private def mkNotifierResource(
      rabbitClient: RabbitClient[IO],
      config: Config
  ): Resource[IO, CoinConfig => Notifier] =
    config.notifier match {
      case NotifierConfig.Lama(exchangeName) =>
        Resource
          .eval(
            config.orchestrator.coins.traverse(c =>
              declareExchangesAndBindings(rabbitClient, c, exchangeName)
            )
          )
          .as { (c: CoinConfig) =>
            new RabbitNotifier(rabbitClient, exchangeName, c.routingKey)
          }
      case NotifierConfig.KNativeRabbitMq(exchangeName) =>
        Resource
          .eval(
            config.orchestrator.coins.traverse(c =>
              declareExchangesAndBindings(rabbitClient, c, exchangeName)
            )
          )
          .as { (c: CoinConfig) =>
            new knative.rabbitmq.RabbitNotifier(rabbitClient, exchangeName, c.routingKey)
          }
      case NotifierConfig.KNativeHttp(uri) =>
        val httpConf = knative.http.HttpNotifierConfig(uri)
        knative.http.HttpNotifier(httpConf).map(notifier => _ => notifier)
    }

  private def mkPublishingQueue(redisClient: RedisClient)(
      f: WorkableEvent[JsonObject] => IO[Unit]
  ): PublishingQueue[UUID, WorkableEvent[JsonObject]] =
    new RedisPublishingQueue[UUID, WorkableEvent[JsonObject]](f, redisClient)

  private def mkEventStream(rabbitClient: RabbitClient[IO], config: OrchestratorConfig)(
      coinConfig: CoinConfig
  ) = {
    val declaredExchangeAndBinding = fs2.Stream.eval(
      declareExchangesAndBindings(rabbitClient, coinConfig, config.lamaEventsExchangeName)
    )

    val eventStream = RabbitUtils
      .createConsumer[ReportableEvent[JsonObject]](
        rabbitClient,
        coinConfig.queueName(config.lamaEventsExchangeName)
      )

    declaredExchangeAndBinding >> eventStream
  }

  // Declare rabbitmq exchanges and bindings used by workers and the orchestrator.
  private def declareExchangesAndBindings(
      rabbit: RabbitClient[IO],
      coinConf: CoinConfig,
      exchangeName: ExchangeName
  ): IO[Unit] = {
    val exchanges = List(
      (exchangeName, ExchangeType.Topic)
    )

    val bindings =
      List(
        (exchangeName, coinConf.routingKey, coinConf.queueName(exchangeName))
      )

    RabbitUtils.declareExchanges(rabbit, exchanges) *>
      RabbitUtils.declareBindings(rabbit, bindings)
  }
}
