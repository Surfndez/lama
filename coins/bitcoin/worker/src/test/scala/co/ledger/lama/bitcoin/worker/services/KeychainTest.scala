package co.ledger.lama.bitcoin.worker.services

import cats.Id
import co.ledger.lama.bitcoin.worker.KeychainFixture
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.util.UUID

import co.ledger.lama.common.logging.DefaultContextLogging

class KeychainTest extends AnyFlatSpec with Matchers with DefaultContextLogging {

  "Keychain.addressesRanges" should "give ranges of the matching size" in {

    val expectedSize = 20

    val firstRanges: List[Range] = Keychain.addressesRanges[Id](expectedSize).take(3).compile.toList

    firstRanges should be(
      List(
        0 to 19,
        20 to 39,
        40 to 59
      )
    )

    all(firstRanges) should have size 20
  }

  "Keychain.addresses" should "give a lookahead size list of addresses" in {

    val knownAddresses = LazyList.from(1).map(_.toString)

    val keychain = new Keychain(KeychainFixture.keychainClient(knownAddresses, lookaheadSize = 2))

    val firstAddresses =
      keychain
        .discoverAddresses(UUID.randomUUID())
        .map(_.map(_.accountAddress))
        .take(3)
        .compile
        .toList
        .unsafeRunSync()

    firstAddresses should be(
      List(
        List("1", "2"),
        List("3", "4"),
        List("5", "6")
      )
    )
  }

}
