package co.ledger.lama.common

import co.ledger.lama.scheduler.domain.models.{Account, AccountGroup, Coin, CoinFamily}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class UuidUtilsSpec extends AnyFunSuite with Matchers {

  test("account identifier to uuid") {
    assert(
      Account("xpub", CoinFamily.Bitcoin, Coin.Btc, AccountGroup("UuidUtilsSpec:23")).id ==
        UUID.fromString("8f90a1b0-1adb-33ff-bcc1-2db04f60e4df")
    )
  }

}
