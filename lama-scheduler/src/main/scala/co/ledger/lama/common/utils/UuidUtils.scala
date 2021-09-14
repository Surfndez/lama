package co.ledger.lama.common.utils

import java.util.UUID

import cats.effect.IO

import scala.util.Try

object UuidUtils {

  case object InvalidUUIDException extends Exception

  def stringToUuidIO(s: String): IO[UUID] =
    IO.fromOption(stringToUuid(s))(InvalidUUIDException)

  def stringToUuid(s: String): Option[UUID] =
    Try(UUID.fromString(s)).toOption

}
