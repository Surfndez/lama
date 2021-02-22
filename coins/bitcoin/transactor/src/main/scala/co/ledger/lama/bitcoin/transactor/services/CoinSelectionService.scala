package co.ledger.lama.bitcoin.transactor.services

import cats.effect.IO
import co.ledger.lama.bitcoin.common.models.interpreter.Utxo
import co.ledger.lama.bitcoin.common.models.transactor.CoinSelectionStrategy

object CoinSelectionService {

  def coinSelection(
      coinSelection: CoinSelectionStrategy,
      utxos: List[Utxo],
      amount: BigInt,
      feesPerUtxo: BigInt
  ): IO[List[Utxo]] =
    coinSelection match {
      case CoinSelectionStrategy.OptimizeSize => optimizeSizePicking(utxos, amount, feesPerUtxo)
      case CoinSelectionStrategy.DepthFirst   => depthFirstPickingRec(utxos, amount, feesPerUtxo)
    }

  private def optimizeSizePicking(
      utxos: List[Utxo],
      targetAmount: BigInt,
      feesPerUtxo: BigInt
  ) =
    depthFirstPickingRec(
      utxos.sortWith(_.value > _.value),
      targetAmount,
      feesPerUtxo
    )

  private def depthFirstPickingRec(
      utxos: List[Utxo],
      targetAmount: BigInt,
      feesPerUtxo: BigInt,
      sum: BigInt = 0
  ): IO[List[Utxo]] = {
    utxos match {
      case utxo :: tail =>
        val effectiveValue = utxo.value - feesPerUtxo

        if (isDust(effectiveValue))
          depthFirstPickingRec(tail, targetAmount, sum, feesPerUtxo)
        else if (sum + effectiveValue > targetAmount)
          IO.pure(List(utxo))
        else
          depthFirstPickingRec(tail, targetAmount, feesPerUtxo, sum + effectiveValue).map(utxo :: _)
      case Nil =>
        IO.raiseError(
          new Exception(
            s"Not enough Utxos to pay for amount: $targetAmount, total sum available: $sum"
          )
        )
    }
  }

  private def isDust(effectiveValue: BigInt): Boolean = {
    effectiveValue < 0
  }

}
