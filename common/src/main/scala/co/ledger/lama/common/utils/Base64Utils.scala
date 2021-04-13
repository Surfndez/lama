package co.ledger.lama.common.utils

import io.circe.parser.parse
import io.circe.{Decoder, Encoder}
import io.circe.syntax._

import java.util.Base64

object Base64Utils {
  def encode[T](value: T)(implicit encoder: Encoder[T]): String =
    Base64.getEncoder.encodeToString(value.asJson.noSpaces.getBytes)

  def decode[T](base64Str: String)(implicit decoder: Decoder[T]): Option[T] =
    parse(new String(Base64.getDecoder.decode(base64Str)))
      .flatMap(_.as[T])
      .toOption
}
