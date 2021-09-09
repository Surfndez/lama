package co.ledger.lama.common.models

import co.ledger.lama.common.utils.Base64Utils
import co.ledger.lama.common.models.implicits._
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import io.circe.parser.parse

import java.util.Base64

final case class PaginationToken[T](state: T, isNext: Boolean) {
  def toBase64(implicit e: Encoder[T]): String =
    Base64Utils.encode(this)
}

object PaginationToken {
  def fromBase64[T](encodedStr: String)(implicit d: Decoder[T]): Option[PaginationToken[T]] =
    parse(new String(Base64.getDecoder.decode(encodedStr)))
      .flatMap(_.as[PaginationToken[T]])
      .toOption

  implicit def encoder[T: Encoder]: Encoder[PaginationToken[T]] =
    deriveConfiguredEncoder[PaginationToken[T]]

  implicit def decoder[T: Decoder]: Decoder[PaginationToken[T]] =
    deriveConfiguredDecoder[PaginationToken[T]]
}
