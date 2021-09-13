package co.ledger.lama.scheduler.config

import co.ledger.lama.common.utils.PostgresConfig
import co.ledger.lama.scheduler.domain.models.{Coin, CoinFamily}
import dev.profunktor.fs2rabbit.config.{Fs2RabbitConfig, Fs2RabbitNodeConfig}
import dev.profunktor.fs2rabbit.model.{ExchangeName, QueueName, RoutingKey}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import pureconfig.module.cats._
import implicits._

import scala.concurrent.duration.FiniteDuration

object config {

  case class Config(
      server: ServerConfig,
      postgres: PostgresConfig,
      orchestrator: OrchestratorConfig,
      rabbit: Fs2RabbitConfig,
      redis: RedisConfig,
      notifier: NotifierConfig
  )

  object Config {
    implicit val configReader: ConfigReader[Config] = deriveReader[Config]
    implicit val rabbitNodeConfigReader: ConfigReader[Fs2RabbitNodeConfig] =
      deriveReader[Fs2RabbitNodeConfig]
    implicit val rabbitConfigReader: ConfigReader[Fs2RabbitConfig] = deriveReader[Fs2RabbitConfig]
  }

  case class RedisConfig(host: String, port: Int, password: String, db: Int, ssl: Boolean)

  object RedisConfig {
    implicit val configReader: ConfigReader[RedisConfig] = deriveReader[RedisConfig]
  }

  case class ServerConfig(
      host: String,
      port: Int
  )

  object ServerConfig {
    implicit val configReader: ConfigReader[ServerConfig] =
      deriveReader[ServerConfig]
  }

  case class OrchestratorConfig(
      lamaEventsExchangeName: ExchangeName,
      coins: List[CoinConfig]
  )

  object OrchestratorConfig {
    implicit val configReader: ConfigReader[OrchestratorConfig] =
      deriveReader[OrchestratorConfig]

    implicit val coinConfigReader: ConfigReader[CoinConfig] =
      deriveReader[CoinConfig]
  }

  case class CoinConfig(
      coinFamily: CoinFamily,
      coin: Coin,
      syncFrequency: FiniteDuration
  ) {
    val routingKey: RoutingKey = RoutingKey(s"$coinFamily.$coin")

    def queueName(exchangeName: ExchangeName): QueueName =
      QueueName(s"${exchangeName.value}.${coinFamily.name}")
  }
}
