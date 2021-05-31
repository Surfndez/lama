package co.ledger.lama.common.clients.grpc

final case class AccountInfoNotFoundException(private val cause: Throwable = None.orNull)
    extends Exception("Account not found", cause)
