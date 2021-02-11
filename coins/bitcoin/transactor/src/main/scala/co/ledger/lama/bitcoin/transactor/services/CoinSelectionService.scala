package co.ledger.lama.bitcoin.transactor.services

import cats.effect.IO
import co.ledger.lama.bitcoin.common.models.interpreter.Utxo
import co.ledger.lama.bitcoin.common.models.transactor.CoinSelectionStrategy

object CoinSelectionService {

  def coinSelection(
      coinSelection: CoinSelectionStrategy,
      utxos: List[Utxo],
      amount: BigInt
  ): IO[List[Utxo]] =
    coinSelection match {
      case CoinSelectionStrategy.OptimizeSize => optimizeSizePicking(utxos, amount)
      case CoinSelectionStrategy.DepthFirst   => depthFirstPickingRec(utxos, amount)
    }

  private def optimizeSizePicking(
      utxos: List[Utxo],
      targetAmount: BigInt
  ) =
    depthFirstPickingRec(
      utxos.sortWith(_.value > _.value),
      targetAmount
    )

  private def depthFirstPickingRec(
      utxos: List[Utxo],
      targetAmount: BigInt,
      sum: BigInt = 0
  ): IO[List[Utxo]] = {
    utxos match {
      case head :: tail =>
        if (isDust(head.value))
          depthFirstPickingRec(tail, targetAmount, sum)
        else if (sum + head.value > targetAmount)
          IO.pure(List(head))
        else
          depthFirstPickingRec(tail, targetAmount, sum + head.value).map(head :: _)
      case Nil =>
        IO.raiseError(
          new Exception(
            s"Not enough Utxos to pay for amount: $targetAmount, total sum available: $sum"
          )
        )
    }
  }

  private def isDust(value: BigInt): Boolean = {
    value < 1000
  }

}
