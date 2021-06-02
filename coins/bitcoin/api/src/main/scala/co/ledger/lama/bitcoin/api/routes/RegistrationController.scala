package co.ledger.lama.bitcoin.api.routes

import cats.effect.{ContextShift, IO}
import cats.implicits._

import co.ledger.lama.bitcoin.api.models.accountManager._
import co.ledger.lama.bitcoin.api.utils.RouterUtils._
import co.ledger.lama.bitcoin.common.clients.grpc.{InterpreterClient, KeychainClient}
import co.ledger.lama.bitcoin.common.utils.CoinImplicits._
import co.ledger.lama.common.clients.grpc.AccountManagerClient
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.models.{Account, AccountGroup}

import io.circe.Json
import io.circe.syntax._

import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.circe.CirceEntityCodec._

object RegistrationController extends Http4sDsl[IO] with ContextLogging {

  implicit val cs: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)

  def registrationRoutes(
      keychainClient: KeychainClient,
      accountManagerClient: AccountManagerClient,
      interpreterClient: InterpreterClient
  ): HttpRoutes[IO] = HttpRoutes.of[IO] {

    // Register account
    case req @ POST -> Root =>
      implicit val lc: LamaLogContext = LamaLogContext()
      val ra = for {
        creationRequest <- req.as[CreationRequest]
        _               <- log.info(s"Creating keychain for account registration")

        createdKeychain <- keychainClient.create(
          creationRequest.accountKey,
          creationRequest.scheme,
          creationRequest.lookaheadSize,
          creationRequest.coin.toNetwork
        )

        account = Account(
          createdKeychain.keychainId.toString,
          creationRequest.coin.coinFamily,
          creationRequest.coin,
          AccountGroup(creationRequest.group)
        )

        logContextWithAccount = lc.withAccount(account)
        _ <- log.info(s"Keychain created")(logContextWithAccount)
        _ <- log.info("Registering account")(logContextWithAccount)
        syncAccount <- accountManagerClient.registerAccount(
          account,
          creationRequest.syncFrequency,
          creationRequest.label
        )

        _ <- log.info(s"Account registered")(logContextWithAccount)
      } yield RegisterAccountResponse(
        syncAccount.accountId,
        syncAccount.syncId,
        createdKeychain.extendedPublicKey
      )

      ra.flatMap(Ok(_))

    // List accounts
    case GET -> Root
        :? OptionalBoundedLimitQueryParamMatcher(limit)
        +& OptionalOffsetQueryParamMatcher(offset) =>
      val t = for {
        boundedLimit <- parseBoundedLimit(limit)

        // Get Account Info
        accountsResult <- accountManagerClient.getAccounts(None, boundedLimit.value, offset)
        accountsWithIds = accountsResult.accounts.map(accountInfo =>
          accountInfo.account.id -> accountInfo
        )

        // Get Balance
        accountsWithBalances <- accountsWithIds.parTraverse { case (accountId, account) =>
          interpreterClient.getBalance(accountId).map(balance => account -> balance)
        }
      } yield (accountsWithBalances, accountsResult.total)

      t.flatMap { case (accountsWithBalances, total) =>
        val accountsInfos = accountsWithBalances.map { case (account, balance) =>
          AccountWithBalance(
            account.account.id,
            account.account.coinFamily,
            account.account.coin,
            account.syncFrequency,
            account.lastSyncEvent,
            balance.balance,
            balance.unconfirmedBalance,
            balance.utxos,
            balance.received,
            balance.sent,
            account.label
          )
        }

        Ok(
          Json.obj(
            "accounts" -> Json.fromValues(accountsInfos.map(_.asJson)),
            "total"    -> Json.fromInt(total)
          )
        )
      }

  }

}
