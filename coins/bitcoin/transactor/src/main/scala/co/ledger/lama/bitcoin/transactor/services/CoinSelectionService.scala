package co.ledger.lama.bitcoin.transactor.services

import cats.effect.IO
import co.ledger.lama.bitcoin.common.models.interpreter.Utxo
import co.ledger.lama.bitcoin.common.models.transactor.CoinSelectionStrategy

import scala.annotation.tailrec

object CoinSelectionService {

//  val defaultMaxUtxos: Int = conf

  def coinSelection(
      coinSelection: CoinSelectionStrategy,
      utxos: List[Utxo],
      amount: BigInt,
      feesPerUtxo: BigInt,
      maxUtxos: Int
  ): IO[List[Utxo]] =
    coinSelection match {
      case CoinSelectionStrategy.OptimizeSize =>
        optimizeSizePicking(utxos, amount, maxUtxos, feesPerUtxo)
      case CoinSelectionStrategy.DepthFirst =>
        depthFirstPickingRec(utxos, amount, maxUtxos, feesPerUtxo)
    }

  private def optimizeSizePicking(
      utxos: List[Utxo],
      targetAmount: BigInt,
      maxUtxos: Int,
      feesPerUtxo: BigInt
  ) =
    depthFirstPickingRec(
      utxos.sortWith(_.value > _.value),
      targetAmount,
      maxUtxos,
      feesPerUtxo
    )

  @tailrec
  private def depthFirstPickingRec(
      utxos: List[Utxo],
      targetAmount: BigInt,
      maxUtxos: Int,
      feesPerUtxo: BigInt,
      sum: BigInt = 0,
      choosenUtxos: List[Utxo] = Nil
  ): IO[List[Utxo]] = {

    if (choosenUtxos.size == maxUtxos)
      IO.raiseError(
        new Throwable(
          s"Couldn't fill transactions with the max number of utxos. Available amount for $maxUtxos is : ${choosenUtxos.map(_.value).sum} minus fees"
        )
      )
    else if (sum > targetAmount)
      IO.pure(choosenUtxos)
    else if (utxos.isEmpty)
      IO.raiseError(
        new Throwable(
          s"Not enough Utxos to pay for amount: $targetAmount, total sum available: $sum"
        )
      )
    else {
      val effectiveValue = utxos.head.value - feesPerUtxo

      depthFirstPickingRec(
        utxos.tail,
        targetAmount,
        maxUtxos,
        feesPerUtxo,
        sum + effectiveValue,
        if (isDust(effectiveValue))
          choosenUtxos
        else
          choosenUtxos ::: List(utxos.head)
      )

    }
  }

  private def isDust(effectiveValue: BigInt): Boolean = {
    effectiveValue < 0
  }

}
