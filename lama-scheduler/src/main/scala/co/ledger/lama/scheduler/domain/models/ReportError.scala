package co.ledger.lama.scheduler.domain.models

import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto._
import co.ledger.lama.scheduler.domain.models.implicits._

case class ReportError(code: String, message: Option[String])

object ReportError {
  def fromThrowable(t: Throwable): ReportError =
    t match {
      case unknown => ReportError(unknown.getClass.getSimpleName, Option(unknown.getMessage))
    }

  implicit val encoder: Encoder[ReportError] = deriveConfiguredEncoder[ReportError]
  implicit val decoder: Decoder[ReportError] = deriveConfiguredDecoder[ReportError]
}
