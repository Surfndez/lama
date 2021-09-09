package co.ledger.lama.common.models

import cats.Show
import java.time.Instant
import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto._
import io.circe.syntax.EncoderOps
import co.ledger.lama.scheduler.protobuf
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.common.utils.{ByteStringUtils, TimestampProtoUtils, UuidUtils}

sealed trait SyncEvent[T] {
  def account: Account
  def syncId: UUID
  def status: Status
  def cursor: Option[T]
  def error: Option[ReportError]
  def time: Instant

  def toProto(implicit enc: Encoder[T]): protobuf.SyncEvent =
    protobuf.SyncEvent(
      account = Some(account.toProto),
      syncId = UuidUtils.uuidToBytes(syncId),
      status = status.name,
      cursor = ByteStringUtils.serialize[T](cursor),
      error = ByteStringUtils.serialize[ReportError](error),
      time = Some(TimestampProtoUtils.serialize(time))
    )
}

object SyncEvent {

  implicit def encoder[T: Encoder]: Encoder[SyncEvent[T]] =
    Encoder.instance {
      case re: ReportableEvent[T]  => re.asJson
      case we: WorkableEvent[T]    => we.asJson
      case te: TriggerableEvent[T] => te.asJson
      case ne: FlaggedEvent[T]     => ne.asJson
    }

  implicit def decoder[T: Decoder]: Decoder[SyncEvent[T]] = {
    Decoder[ReportableEvent[T]]
      .map[SyncEvent[T]](identity)
      .or(Decoder[WorkableEvent[T]].map[SyncEvent[T]](identity))
      .or(Decoder[TriggerableEvent[T]].map[SyncEvent[T]](identity))
      .or(Decoder[FlaggedEvent[T]].map[SyncEvent[T]](identity))
  }

  def apply[T](
      account: Account,
      syncId: UUID,
      status: Status,
      cursor: Option[T],
      error: Option[ReportError],
      time: Instant
  ): SyncEvent[T] =
    status match {
      case ws: WorkableStatus =>
        WorkableEvent(account, syncId, ws, cursor, error, time)
      case rs: ReportableStatus =>
        ReportableEvent(account, syncId, rs, cursor, error, time)
      case ts: TriggerableStatus =>
        TriggerableEvent(account, syncId, ts, cursor, error, time)
      case ns: FlaggedStatus =>
        FlaggedEvent(account, syncId, ns, cursor, error, time)
    }

  def fromProto[T](proto: protobuf.SyncEvent)(implicit dec: Decoder[T]): SyncEvent[T] =
    SyncEvent[T](
      Account.fromProto(proto.account.get),
      UuidUtils.bytesToUuid(proto.syncId).get,
      Status.fromKey(proto.status).get,
      ByteStringUtils.deserialize[T](proto.cursor),
      ByteStringUtils.deserialize[ReportError](proto.error),
      TimestampProtoUtils.deserialize(proto.time.get)
    )

}

trait WithBusinessId[K] { def businessId: K }

case class WorkableEvent[T](
    account: Account,
    syncId: UUID,
    status: WorkableStatus,
    cursor: Option[T],
    error: Option[ReportError],
    time: Instant
) extends SyncEvent[T]
    with WithBusinessId[UUID] {

  val businessId: UUID = account.id

  def asPublished: FlaggedEvent[T] =
    FlaggedEvent[T](
      account,
      syncId,
      Status.Published,
      cursor,
      error,
      Instant.now()
    )

  // TODO: newCursor should not be optional
  def asReportableSuccessEvent(newCursor: Option[T]): ReportableEvent[T] =
    ReportableEvent(
      account,
      syncId,
      status.success,
      newCursor,
      None,
      Instant.now()
    )

  def asReportableFailureEvent(error: ReportError): ReportableEvent[T] = {
    ReportableEvent(
      account,
      syncId,
      status.failure,
      cursor,
      Some(error),
      Instant.now()
    )
  }
}

object WorkableEvent {
  implicit def encoder[T: Encoder]: Encoder[WorkableEvent[T]] =
    deriveConfiguredEncoder[WorkableEvent[T]]

  implicit def decoder[T: Decoder]: Decoder[WorkableEvent[T]] =
    deriveConfiguredDecoder[WorkableEvent[T]]
}

case class ReportableEvent[T](
    account: Account,
    syncId: UUID,
    status: ReportableStatus,
    cursor: Option[T],
    error: Option[ReportError],
    time: Instant
) extends SyncEvent[T]

object ReportableEvent {
  implicit def encoder[T: Encoder]: Encoder[ReportableEvent[T]] =
    deriveConfiguredEncoder[ReportableEvent[T]]

  implicit def decoder[T: Decoder]: Decoder[ReportableEvent[T]] =
    deriveConfiguredDecoder[ReportableEvent[T]]

  implicit def showMessage[T: Encoder]: Show[ReportableEvent[T]] =
    Show.show(reportMsg => s"${reportMsg.account}:")
}

case class TriggerableEvent[T](
    account: Account,
    syncId: UUID,
    status: TriggerableStatus,
    cursor: Option[T],
    error: Option[ReportError],
    time: Instant
) extends SyncEvent[T] {
  def nextWorkable: WorkableEvent[T] =
    WorkableEvent[T](
      account,
      UUID.randomUUID(),
      status.nextWorkable,
      cursor,
      error,
      Instant.now()
    )
}

object TriggerableEvent {
  implicit def encoder[T: Encoder]: Encoder[TriggerableEvent[T]] =
    deriveConfiguredEncoder[TriggerableEvent[T]]

  implicit def decoder[T: Decoder]: Decoder[TriggerableEvent[T]] =
    deriveConfiguredDecoder[TriggerableEvent[T]]
}

case class FlaggedEvent[T](
    account: Account,
    syncId: UUID,
    status: FlaggedStatus,
    cursor: Option[T],
    error: Option[ReportError],
    time: Instant
) extends SyncEvent[T]

object FlaggedEvent {
  implicit def encoder[T: Encoder]: Encoder[FlaggedEvent[T]] =
    deriveConfiguredEncoder[FlaggedEvent[T]]

  implicit def decoder[T: Decoder]: Decoder[FlaggedEvent[T]] =
    deriveConfiguredDecoder[FlaggedEvent[T]]
}
