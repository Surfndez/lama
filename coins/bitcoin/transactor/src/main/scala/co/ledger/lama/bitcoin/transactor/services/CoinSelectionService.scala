package co.ledger.lama.bitcoin.transactor.services

import co.ledger.lama.bitcoin.common.models.interpreter.Utxo
import co.ledger.lama.bitcoin.common.models.transactor.CoinSelectionStrategy

import scala.annotation.tailrec

object CoinSelectionService {

  sealed trait CoinSerlectionError extends Throwable

  case class NotEnoughUtxos(maxUtxos: Int, maxAmount: BigInt) extends CoinSerlectionError {
    override def getMessage: String =
      s"Couldn't fill transactions with the max number of utxos. Available amount for $maxUtxos utxos is $maxAmount Satoshis minus fees"
  }

  case class NotEnoughFunds(maxAmount: BigInt) extends CoinSerlectionError {
    override def getMessage: String =
      s"Not enough funds to pay for transaction amount, total sum available: $maxAmount Satoshis minus fees"
  }

  def coinSelection(
      coinSelection: CoinSelectionStrategy,
      utxos: List[Utxo],
      amount: BigInt,
      feesPerUtxo: BigInt,
      maxUtxos: Int
  ): Either[CoinSerlectionError, List[Utxo]] =
    coinSelection match {
      case CoinSelectionStrategy.OptimizeSize =>
        optimizeSizePicking(utxos, amount, maxUtxos, feesPerUtxo)
      case CoinSelectionStrategy.DepthFirst =>
        depthFirstPicking(utxos, amount, maxUtxos, feesPerUtxo)
      case CoinSelectionStrategy.MergeOutputs =>
        mergeOutputsPicking(utxos, amount, maxUtxos, feesPerUtxo)
    }

  private def optimizeSizePicking(
      utxos: List[Utxo],
      targetAmount: BigInt,
      maxUtxos: Int,
      feesPerUtxo: BigInt
  ): Either[CoinSerlectionError, List[Utxo]] =
    depthFirstPicking(
      utxos.sortWith(_.value > _.value),
      targetAmount,
      maxUtxos,
      feesPerUtxo
    )

  private def depthFirstPicking(
      utxos: List[Utxo],
      targetAmount: BigInt,
      maxUtxos: Int,
      feesPerUtxo: BigInt
  ): Either[CoinSerlectionError, List[Utxo]] = {

    @tailrec
    def depthFirstPickingRec(
        utxos: List[Utxo],
        sum: BigInt,
        chosenUtxos: List[Utxo]
    ): Either[CoinSerlectionError, List[Utxo]] =
      if (sum > targetAmount)
        Right(chosenUtxos.reverse)
      else if (chosenUtxos.size == maxUtxos)
        Left(NotEnoughUtxos(maxUtxos, chosenUtxos.map(_.value).sum))
      else if (utxos.isEmpty)
        Left(NotEnoughFunds(chosenUtxos.map(_.value).sum))
      else {
        val effectiveValue = utxos.head.value - feesPerUtxo

        depthFirstPickingRec(
          utxos.tail,
          sum + effectiveValue,
          if (isDust(effectiveValue))
            chosenUtxos
          else
            utxos.head :: chosenUtxos
        )
      }

    depthFirstPickingRec(utxos, 0, Nil)
  }

  private def mergeOutputsPicking(
      utxos: List[Utxo],
      targetAmount: BigInt,
      maxUtxos: Int,
      feesPerUtxo: BigInt
  ): Either[CoinSerlectionError, List[Utxo]] = {

    @tailrec
    def mergeOutputsPickingRec(
        utxos: List[Utxo],
        chosenUtxos: List[Utxo]
    ): Either[CoinSerlectionError, List[Utxo]] =
      if (
        chosenUtxos.map(_.value).sum >= targetAmount ||
        chosenUtxos.size == maxUtxos ||
        utxos.isEmpty
      )
        Right(chosenUtxos.reverse)
      else
        mergeOutputsPickingRec(
          utxos.tail,
          if (isNearDust(utxos.head.value - feesPerUtxo))
            utxos.head :: chosenUtxos
          else
            chosenUtxos
        )

    mergeOutputsPickingRec(utxos, Nil)
  }

  private def isDust(effectiveValue: BigInt): Boolean = {
    effectiveValue < 0
  }

  private def isNearDust(effectiveValue: BigInt): Boolean = {
    effectiveValue > 0 && effectiveValue < 1000
  }

}
