package co.ledger.lama.bitcoin.worker.models

import co.ledger.lama.bitcoin.common.models.explorer.Transaction
import co.ledger.lama.bitcoin.common.models.interpreter.AccountAddress

case class BatchResult[Tx <: Transaction](
    addresses: List[AccountAddress],
    transactions: List[Tx]
)
