package co.ledger.lama.bitcoin.api.middlewares

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import co.ledger.lama.bitcoin.api.models.ApiContext
import co.ledger.lama.common.clients.grpc.AccountManagerClient
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.models.Status.Deleted
import co.ledger.lama.manager.Exceptions.AccountNotFoundException
import org.http4s._
import org.http4s.dsl.impl._
import org.http4s.server.ContextMiddleware

case class AccountMiddleware(accountManagerClient: AccountManagerClient)
    extends ContextMiddleware[IO, ApiContext]
    with ContextLogging {

  type MyIO[A] = OptionT[IO, A]

  def apply(
      service: Kleisli[MyIO, ContextRequest[IO, ApiContext], Response[IO]]
  ): Kleisli[MyIO, Request[IO], Response[IO]] =
    Kleisli[MyIO, Request[IO], ContextRequest[IO, ApiContext]] {

      case req @ _ -> UUIDVar(accountId) /: _ =>
        implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)

        OptionT(
          accountManagerClient
            .getAccountInfo(accountId)
            .map { accountInfo =>
              accountInfo.lastSyncEvent.map(event => event.status) match {
                case Some(Deleted) =>
                  log.info("Account deleted")
                  None
                case _ => Option(ContextRequest(ApiContext(accountInfo.account), req))
              }
            }
            .handleErrorWith {
              case AccountNotFoundException(_) =>
                log.info("Account not found in AccountMiddleWare")
                IO(None)
              case err =>
                log.error("Error while fetching account info in AccountMiddleWare", err)
                IO(None)
            }
        )

    }.andThen(service)

}
