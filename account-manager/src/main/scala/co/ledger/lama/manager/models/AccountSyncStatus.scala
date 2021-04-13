package co.ledger.lama.manager.models

import java.time.Instant
import java.util.UUID

import co.ledger.lama.common.models.{Account, ReportError, Status}
import io.circe.JsonObject

case class AccountSyncStatus(
    account: Account,
    syncFrequency: Long,
    label: Option[String],
    syncId: UUID,
    status: Status,
    cursor: Option[JsonObject],
    error: Option[ReportError],
    updated: Instant
)
