package co.ledger.lama.bitcoin.interpreter.services

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import co.ledger.lama.bitcoin.common.models.interpreter.{AccountAddress, ChangeType}
import doobie._
import doobie.implicits._

import java.util.UUID

class FlaggingService(db: Transactor[IO]) {

  def flagInputsAndOutputs(
      accountId: UUID,
      accountAddresses: List[AccountAddress]
  ): IO[Unit] = {
    val (internalAddresses, externalAddresses) =
      accountAddresses
        .partition(_.changeType == ChangeType.Internal)

    val flagInputs =
      NonEmptyList
        .fromList(accountAddresses)
        .map { addresses =>
          OperationQueries
            .flagBelongingInputs(
              accountId,
              addresses
            )
            .transact(db)
        }
        .getOrElse(IO.pure(0))

    // Flag outputs with known addresses and update address type (INTERNAL / EXTERNAL)
    val flagInternalOutputs =
      NonEmptyList
        .fromList(internalAddresses)
        .map { addresses =>
          OperationQueries
            .flagBelongingOutputs(
              accountId,
              addresses,
              ChangeType.Internal
            )
            .transact(db)
        }
        .getOrElse(IO.pure(0))

    val flagExternalOutputs =
      NonEmptyList
        .fromList(externalAddresses)
        .map { addresses =>
          OperationQueries
            .flagBelongingOutputs(
              accountId,
              addresses,
              ChangeType.External
            )
            .transact(db)
        }
        .getOrElse(IO.pure(0))

    (flagInputs, flagInternalOutputs, flagExternalOutputs).parTupled.void
  }

}
