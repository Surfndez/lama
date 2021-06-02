package co.ledger.lama.bitcoin.api.models

import java.util.UUID

import co.ledger.lama.common.models.Account

case class ApiContext(
    account: Account,
    followUpId: UUID = UUID.randomUUID()
)
