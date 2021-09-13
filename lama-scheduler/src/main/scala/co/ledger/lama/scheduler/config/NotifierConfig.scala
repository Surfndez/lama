package co.ledger.lama.scheduler.config

import dev.profunktor.fs2rabbit.model.ExchangeName
import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import implicits._

sealed abstract class NotifierConfig extends Product with Serializable

object NotifierConfig {

  final case class Lama(exchangeName: ExchangeName) extends NotifierConfig

  object Lama {
    implicit val lamaReader: ConfigReader[Lama] =
      deriveReader[Lama]
  }

  final case class KNativeHttp(uri: Uri) extends NotifierConfig

  object KNativeHttp {
    implicit val knativeHttpReader: ConfigReader[KNativeHttp] =
      deriveReader[KNativeHttp]
  }

  final case class KNativeRabbitMq(exchangeName: ExchangeName) extends NotifierConfig

  object KNativeRabbitMq {
    implicit val knativeRabbitMqReader: ConfigReader[KNativeRabbitMq] =
      deriveReader[KNativeRabbitMq]
  }

  implicit lazy val notifierConfigReader: ConfigReader[NotifierConfig] =
    deriveReader[NotifierConfig]
}
