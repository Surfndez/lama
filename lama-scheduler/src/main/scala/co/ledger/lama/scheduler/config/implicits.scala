package co.ledger.lama.scheduler.config

import dev.profunktor.fs2rabbit.model.ExchangeName
import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import cats.implicits._

object implicits {
  implicit val exchangeNameConfigReader: ConfigReader[ExchangeName] =
    ConfigReader.fromString(str => Right(ExchangeName(str)))

  implicit val uriReader: ConfigReader[Uri] = ConfigReader[String].emap { s =>
    Uri.fromString(s).leftMap(t => CannotConvert(s, "Uri", t.getMessage))
  }
}
