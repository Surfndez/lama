package co.ledger.lama.bitcoin.worker.services

import cats.effect.{ContextShift, IO, Timer}
import co.ledger.lama.bitcoin.common.clients.grpc.InterpreterClient
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.models.explorer.{
  ConfirmedTransaction,
  DefaultInput,
  Transaction,
  UnconfirmedTransaction
}
import co.ledger.lama.bitcoin.common.models.interpreter.AccountAddress
import co.ledger.lama.bitcoin.worker.models.BatchResult
import co.ledger.lama.bitcoin.worker.services.Bookkeeper.{BlockHash, Recordable}
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models.Coin
import fs2.{Chunk, Pull, Stream}

import java.util.UUID

class Bookkeeper(
    keychain: Keychain,
    explorerClient: Coin => ExplorerClient,
    interpreterClient: InterpreterClient,
    maxTxsToSavePerBatch: Int,
    maxConcurrent: Int
) extends IOLogging {

  def record[Tx <: Transaction: Recordable](
      coin: Coin,
      accountId: UUID,
      keychainId: UUID,
      blockHash: Option[BlockHash]
  )(implicit cs: ContextShift[IO]): Stream[IO, BatchResult[Tx]] = {

    def recordTransactionsWithAddresses(
        keychainAddresses: Stream[IO, List[AccountAddress]]
    ): Pull[IO, BatchResult[Tx], Unit] = {

      keychainAddresses.pull.uncons1
        .flatMap {

          case None => Pull.done
          case Some((addresses, remainingAddresses)) =>
            Pull
              .eval {
                for {

                  _ <- log.info("Fetching transactions from explorer")
                  transactions <- Bookkeeper
                    .fetch[Tx]
                    .apply(explorerClient(coin))(
                      addresses.map(_.accountAddress).toSet,
                      blockHash
                    )
                    .prefetch
                    .chunkN(maxTxsToSavePerBatch)
                    .map(_.toList)
                    .parEvalMapUnordered(maxConcurrent) { txs =>
                      Bookkeeper
                        .save[Tx]
                        .apply(interpreterClient)(accountId, txs)
                        .flatMap(log.info)
                        .as(txs)
                    }
                    .flatMap(Stream.emits(_))
                    .compile
                    .toList

                  usedAddresses = addresses.filter { a =>
                    transactions.exists { t =>
                      val isInputAddress = t.inputs.collectFirst {
                        case i: DefaultInput if i.address == a.accountAddress => i
                      }.isDefined

                      isInputAddress || t.outputs.exists(_.address == a.accountAddress)
                    }
                  }

                  _ <-
                    if (transactions.nonEmpty) {
                      // Mark addresses as used.
                      log.info(s"Marking addresses as used") *>
                        keychain.markAsUsed(
                          keychainId,
                          usedAddresses.map(_.accountAddress).toSet
                        )
                    } else IO.unit

                } yield BatchResult(usedAddresses, transactions)
              }
              .flatMap { batchResult =>
                if (batchResult.transactions.isEmpty) {
                  Pull.done
                } else {
                  Pull.output(Chunk(batchResult)) >>
                    recordTransactionsWithAddresses(
                      remainingAddresses
                    )
                }
              }
        }
    }

    recordTransactionsWithAddresses(
      keychain.addresses(keychainId)
    ).stream
  }
}

object Bookkeeper {
  type AccountId = UUID
  type Address   = String
  type BlockHash = String
  type Fetch[Tx <: Transaction] =
    ExplorerClient => (Set[Address], Option[BlockHash]) => Stream[IO, Tx]
  type Save[Tx <: Transaction] = InterpreterClient => (AccountId, List[Tx]) => IO[String]

  def save[Tx <: Transaction](implicit recordable: Recordable[Tx]): Save[Tx]   = recordable.save
  def fetch[Tx <: Transaction](implicit recordable: Recordable[Tx]): Fetch[Tx] = recordable.fetch

  trait Recordable[Tx <: Transaction] {
    val fetch: Fetch[Tx]
    val save: Save[Tx]
  }

  implicit def confirmed(implicit
      cs: ContextShift[IO],
      t: Timer[IO]
  ): Recordable[ConfirmedTransaction] =
    new Recordable[ConfirmedTransaction] {
      override val fetch: Fetch[ConfirmedTransaction] =
        explorer => { case (addresses, b) => explorer.getConfirmedTransactions(addresses.toSeq, b) }

      override val save: Save[ConfirmedTransaction] = i => { case (a, txs) =>
        for {
          savedTxsCount <- i.saveTransactions(a, txs)
        } yield s"$savedTxsCount new transactions saved from blockchain"
      }
    }

  implicit def unconfirmedTransaction(implicit
      cs: ContextShift[IO],
      t: Timer[IO]
  ): Recordable[UnconfirmedTransaction] =
    new Recordable[UnconfirmedTransaction] {
      override val fetch: Fetch[UnconfirmedTransaction] = explorer => { case (addresses, _) =>
        explorer.getUnconfirmedTransactions(addresses)
      }

      override val save: Save[UnconfirmedTransaction] = i => { case (a, txs) =>
        for {
          savedTxsCount <- i.saveUnconfirmedTransactions(a, txs)
        } yield s"$savedTxsCount new transactions saved from mempool"
      }
    }

}
