package co.ledger.lama.scheduler

import java.util.UUID

import co.ledger.lama.common.models.{Coin, CoinFamily}

object Exceptions {

  case class CoinConfigurationException(coinFamily: CoinFamily, coin: Coin)
      extends Exception(s"Could not found config for $coinFamily - $coin")

  case object RedisUnexpectedException extends Exception("Unexpected exception from Redis")

  case class MalformedProtobufException(message: scalapb.GeneratedMessage)
      extends Exception(s"Malformed protobuf: ${message.toProtoString}")

  case class AccountNotFoundException(accountId: UUID)
      extends Exception(s"Account not found: $accountId")

}
