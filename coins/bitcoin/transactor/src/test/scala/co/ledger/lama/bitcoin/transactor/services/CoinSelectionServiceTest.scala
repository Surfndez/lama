package co.ledger.lama.bitcoin.transactor.services

import java.time.Instant

import cats.data.NonEmptyList
import co.ledger.lama.bitcoin.common.models.interpreter.{ChangeType, Utxo}
import co.ledger.lama.bitcoin.common.models.transactor.CoinSelectionStrategy
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class CoinSelectionServiceTest extends AnyFlatSpecLike with Matchers {

  "A depthFirst Strategy" should "return oldest utxos first" in {

    val utxo = Utxo(
      "hash",
      0,
      100000,
      "address",
      "script",
      Some(ChangeType.Internal),
      NonEmptyList.of(0, 1),
      Instant.now
    )

    val utxos = List(
      utxo,
      utxo.copy(value = 10000)
    )

    CoinSelectionService
      .coinSelection(CoinSelectionStrategy.DepthFirst, utxos, 10000)
      .unsafeRunSync() should have size 1

    CoinSelectionService
      .coinSelection(CoinSelectionStrategy.DepthFirst, utxos, 100001)
      .unsafeRunSync() should have size 2

  }

  "A optimizeSize Strategy" should "biggest utxos first" in {

    val utxo = Utxo(
      "hash",
      0,
      1000,
      "address",
      "script",
      Some(ChangeType.Internal),
      NonEmptyList.of(0, 1),
      Instant.now
    )

    val utxos = List(
      utxo.copy(value = 5000),
      utxo.copy(value = 10000),
      utxo.copy(value = 40000),
      utxo.copy(value = 2000),
      utxo.copy(value = 20000)
    )

    val firstSelection = CoinSelectionService
      .coinSelection(CoinSelectionStrategy.OptimizeSize, utxos, 10000)
      .unsafeRunSync()

    firstSelection should have size 1
    firstSelection.head.value shouldBe 40000

    val secondSelection = CoinSelectionService
      .coinSelection(CoinSelectionStrategy.OptimizeSize, utxos, 50000)
      .unsafeRunSync()

    secondSelection should have size 2
    secondSelection.head.value shouldBe 40000
    secondSelection.last.value shouldBe 20000
  }

}
