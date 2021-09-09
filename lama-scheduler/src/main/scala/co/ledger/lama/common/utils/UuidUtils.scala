package co.ledger.lama.common.utils

import java.nio.ByteBuffer
import java.util.UUID

import cats.effect.IO
import com.google.protobuf.ByteString

import scala.util.Try

object UuidUtils {

  case object InvalidUUIDException extends Exception

  def uuidToBytes(uuid: UUID): ByteString = {
    val buffer = ByteBuffer.allocate(16) // uuid = 16 bits
    buffer.putLong(uuid.getMostSignificantBits)  // put the most sig bits first
    buffer.putLong(uuid.getLeastSignificantBits) // then put the least sig bits
    buffer.rewind()                              // rewind: the position is set to zero and the mark is discarded
    ByteString.copyFrom(buffer)
  }

  def unsafeBytesToUuid(bytes: ByteString): UUID = {
    UuidUtils
      .bytesToUuid(bytes)
      .getOrElse(throw UuidUtils.InvalidUUIDException)
  }

  def bytesToUuid(bytes: ByteString): Option[UUID] = {
    val buffer = bytes.asReadOnlyByteBuffer()
    if (buffer.capacity() != 16) None
    else Some(new UUID(buffer.getLong(0), buffer.getLong(8)))
  }

  def bytesToUuidIO(bytes: ByteString): IO[UUID] =
    IO.fromOption(bytesToUuid(bytes))(InvalidUUIDException)

  def stringToUuidIO(s: String): IO[UUID] =
    IO.fromOption(stringToUuid(s))(InvalidUUIDException)

  def stringToUuid(s: String): Option[UUID] =
    Try(UUID.fromString(s)).toOption

}
