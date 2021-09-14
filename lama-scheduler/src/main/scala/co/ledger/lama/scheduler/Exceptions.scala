package co.ledger.lama.scheduler

import java.util.UUID
import co.ledger.lama.scheduler.domain.models.{Coin, CoinFamily}

object Exceptions {

  case class CoinConfigurationException(coinFamily: CoinFamily, coin: Coin)
      extends Exception(s"Could not found config for $coinFamily - $coin")

  case object RedisUnexpectedException extends Exception("Unexpected exception from Redis")

  case class AccountNotFoundException(accountId: UUID)
      extends Exception(s"Account not found: $accountId")

}
