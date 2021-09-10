package co.ledger.lama.scheduler.domain.models

import java.time.Instant
import java.util.UUID
import cats.implicits._
import doobie.util.meta.Meta
import doobie.postgres.implicits._
import doobie.util.{Get, Put, Read}
import io.circe.generic.extras.Configuration
import io.circe.parser.parse
import io.circe.{Decoder, Encoder, Json, JsonObject}
import io.circe.syntax._
import org.postgresql.util.PGobject

object implicits {

  implicit val defaultCirceConfig: Configuration =
    Configuration.default.withSnakeCaseConstructorNames.withSnakeCaseMemberNames

  implicit val jsonMeta: Meta[Json] =
    Meta.Advanced
      .other[PGobject]("json")
      .timap[Json](a => parse(a.getValue).leftMap[Json](e => throw e).merge)(a => {
        val o = new PGobject
        o.setType("json")
        o.setValue(a.noSpaces)
        o
      })

  implicit val uuidEncoder: Encoder[UUID] = Encoder.encodeString.contramap(_.toString)
  implicit val uuidDecoder: Decoder[UUID] = Decoder.decodeString.map(UUID.fromString)

  implicit val jsonObjectGet: Get[Option[JsonObject]] =
    jsonMeta.get.map(_.asObject)

  implicit val jsonObjectPut: Put[Option[JsonObject]] =
    jsonMeta.put.contramap(_.map(Json.fromJsonObject).getOrElse(Json.Null))

  implicit val meta: Meta[Status] =
    pgEnumStringOpt("sync_status", Status.fromKey, _.name)

  implicit val workableStatusMeta: Meta[WorkableStatus] =
    pgEnumStringOpt("sync_status", WorkableStatus.fromKey, _.name)

  implicit val reportableStatusMeta: Meta[ReportableStatus] =
    pgEnumStringOpt("sync_status", ReportableStatus.fromKey, _.name)

  implicit val flaggedStatusMeta: Meta[FlaggedStatus] =
    pgEnumStringOpt("sync_status", FlaggedStatus.fromKey, _.name)

  implicit val triggerableStatusMeta: Meta[TriggerableStatus] =
    pgEnumStringOpt("sync_status", TriggerableStatus.fromKey, _.name)

  implicit val coinMeta: Meta[Coin] =
    pgEnumStringOpt("coin", Coin.fromKey, _.name)

  implicit val coinFamilyMeta: Meta[CoinFamily] =
    pgEnumStringOpt("coin_family", CoinFamily.fromKey, _.name)

  implicit lazy val syncEventRead: Read[SyncEvent[JsonObject]] =
    Read[
      (Account, UUID, Status, Option[JsonObject], Option[JsonObject], Instant)
    ].map { case (account, syncId, status, cursor, error, updated) =>
      SyncEvent(
        account,
        syncId,
        status,
        cursor,
        error.flatMap(_.asJson.as[ReportError].toOption),
        updated
      )
    }

  implicit lazy val workableEventRead: Read[WorkableEvent[JsonObject]] =
    Read[
      (Account, UUID, WorkableStatus, Option[JsonObject], Option[JsonObject], Instant)
    ].map { case (account, syncId, status, cursor, error, updated) =>
      WorkableEvent(
        account,
        syncId,
        status,
        cursor,
        error.flatMap(_.asJson.as[ReportError].toOption),
        updated
      )
    }

  implicit lazy val triggerableEventRead: Read[TriggerableEvent[JsonObject]] =
    Read[
      (Account, UUID, TriggerableStatus, Option[JsonObject], Option[JsonObject], Instant)
    ].map { case (account, syncId, status, cursor, error, updated) =>
      TriggerableEvent(
        account,
        syncId,
        status,
        cursor,
        error.flatMap(_.asJson.as[ReportError].toOption),
        updated
      )
    }

  implicit val accountGroupRead: Read[AccountGroup] =
    Read[String].map(AccountGroup(_))

  implicit val accountInfoRead: Read[AccountInfo] =
    Read[(Account, Long, Option[String])].map { case (account, syncFrequency, label) =>
      AccountInfo(
        account,
        syncFrequency,
        None,
        label
      )
    }

  implicit val accountSyncStatusRead: Read[AccountSyncStatus] =
    Read[
      (
          Account,
          Long,
          Option[String],
          UUID,
          Status,
          Option[JsonObject],
          Option[JsonObject],
          Instant
      )
    ].map {
      case (
            account,
            syncFrequency,
            label,
            syncId,
            status,
            cursor,
            error,
            updated
          ) =>
        AccountSyncStatus(
          account,
          syncFrequency,
          label,
          syncId,
          status,
          cursor,
          error.flatMap(_.asJson.as[ReportError].toOption),
          updated
        )
    }
}
