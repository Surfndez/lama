package co.ledger.lama.bitcoin.api.routes

import cats.Monoid
import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import co.ledger.lama.bitcoin.api.models.BalancePreset
import co.ledger.lama.bitcoin.api.models.accountManager._
import co.ledger.lama.bitcoin.api.models.transactor._
import co.ledger.lama.bitcoin.api.{models => apiModels}
import co.ledger.lama.bitcoin.api.utils.RouterUtils._
import co.ledger.lama.bitcoin.common.clients.grpc.{
  InterpreterClient,
  KeychainClient,
  TransactorClient
}
import co.ledger.lama.common.models.implicits.defaultCirceConfig
import co.ledger.lama.bitcoin.common.models.interpreter.ChangeType
import co.ledger.lama.bitcoin.common.utils.CoinImplicits._
import co.ledger.lama.common.clients.grpc.AccountManagerClient
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}
import co.ledger.lama.common.utils.UuidUtils
import io.circe.Json
import io.circe.generic.extras.auto._
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import co.ledger.lama.common.models.{Account, AccountGroup}

object AccountController extends Http4sDsl[IO] with ContextLogging {

  def transactionsRoutes(
      keychainClient: KeychainClient,
      accountManagerClient: AccountManagerClient,
      transactorClient: TransactorClient
  ): HttpRoutes[IO] = HttpRoutes.of[IO] {

    // Create transaction
    case req @ POST -> Root / UUIDVar(
          accountId
        ) / "transactions" =>
      implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
      (for {
        _                           <- log.info(s"Preparing transaction creation")
        apiCreateTransactionRequest <- req.as[CreateTransactionRequest]

        accountInfo <- accountManagerClient.getAccountInfo(accountId)
        keychainId  <- UuidUtils.stringToUuidIO(accountInfo.account.identifier)

        internalResponse <- transactorClient
          .createTransaction(
            accountId,
            keychainId,
            accountInfo.account.coin,
            apiCreateTransactionRequest.coinSelection,
            apiCreateTransactionRequest.outputs.map(_.toCommon),
            apiCreateTransactionRequest.feeLevel,
            apiCreateTransactionRequest.customFeePerKb,
            apiCreateTransactionRequest.maxUtxos
          )

        apiUtxosDerivations = internalResponse.utxos.map(_.derivation.toList)
        apiUtxosPublicKeys <- keychainClient.getAddressesPublicKeys(keychainId, apiUtxosDerivations)
        apiUtxos = internalResponse.utxos
          .zip(apiUtxosPublicKeys)
          .map { case (commonUtxo, pubKey) =>
            apiModels.SpendableTxo.fromCommon(commonUtxo, pubKey)
          }
        response = CreateTransactionResponse.fromCommon(internalResponse, apiUtxos)
      } yield response).flatMap(Ok(_))

    // Sign transaction (only for testing)
    case req @ POST -> Root / "_internal" / "sign" =>
      implicit val lc: LamaLogContext = LamaLogContext()
      for {
        _       <- log.info(s"Signing Transaction")
        request <- req.as[GenerateSignaturesRequest]

        response <- transactorClient
          .generateSignatures(
            request.rawTransaction,
            request.utxos.map(_.toCommon),
            request.privKey
          )
          .flatMap(Ok(_))

      } yield response

    // Send transaction
    case req @ POST -> Root / UUIDVar(
          accountId
        ) / "transactions" / "send" =>
      implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
      for {
        _       <- log.info(s"Broadcasting transaction for account: $accountId")
        request <- req.as[BroadcastTransactionRequest]

        accountInfo <- accountManagerClient.getAccountInfo(accountId)
        keychainId  <- UuidUtils.stringToUuidIO(accountInfo.account.identifier)

        txInfo <- transactorClient
          .broadcastTransaction(
            keychainId,
            accountInfo.account.coin.name,
            request.rawTransaction,
            request.derivations,
            request.signatures
          )
          .flatMap(Ok(_))

        syncResult <- accountManagerClient.resyncAccount(accountId, wipe = false)
        _          <- log.info(s"Sync sent to account manager: $syncResult")

      } yield txInfo

    case req @ POST -> Root / UUIDVar(
          accountId
        ) / "recipients" =>
      implicit val context: LamaLogContext = LamaLogContext().withAccountId(accountId)
      for {
        account   <- accountManagerClient.getAccountInfo(accountId).map(_.account)
        addresses <- req.as[NonEmptyList[String]]
        _ <- log.info(s"Validating addresses : ${addresses.toList.mkString(", ")}")(
          context.withAccount(account)
        )
        result <- transactorClient
          .validateAddresses(account.coin, addresses.map(TransactorClient.Address))
          .map(results =>
            results.collectFold {
              case TransactorClient.Accepted(address) => ValidationResult.valid(address)
              case TransactorClient.Rejected(address, reason) =>
                ValidationResult.invalid(address, reason)
            }
          )
          .flatMap(Ok(_))

      } yield result
  }

  def routes(
      keychainClient: KeychainClient,
      accountManagerClient: AccountManagerClient,
      interpreterClient: InterpreterClient
  ): HttpRoutes[IO] =
    HttpRoutes.of[IO] {

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

      // Get account info
      case GET -> Root / UUIDVar(accountId) =>
        accountManagerClient
          .getAccountInfo(accountId)
          .parProduct(interpreterClient.getBalance(accountId))
          .flatMap { case (accountInfo, balance) =>
            Ok(
              AccountWithBalance(
                accountInfo.account.id,
                accountInfo.account.coinFamily,
                accountInfo.account.coin,
                accountInfo.syncFrequency,
                accountInfo.lastSyncEvent,
                balance.balance,
                balance.unconfirmedBalance,
                balance.utxos,
                balance.received,
                balance.sent,
                accountInfo.label
              )
            )
          }

      // Get account events
      case GET -> Root / UUIDVar(accountId) / "events"
          :? OptionalBoundedLimitQueryParamMatcher(limit)
          +& OptionalOffsetQueryParamMatcher(offset)
          +& OptionalSortQueryParamMatcher(sort) =>
        implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
        for {
          boundedLimit <- parseBoundedLimit(limit)
          _            <- log.info("Get Events")
          res <- accountManagerClient
            .getSyncEvents(accountId, boundedLimit.value, offset, sort)
            .flatMap(Ok(_))
        } yield res

      // Update account
      case req @ PUT -> Root / UUIDVar(accountId) =>
        implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
        val r = for {
          updateRequest <- req.as[UpdateRequest]

          _ <- log.info(
            s"Updating account with : $updateRequest"
          )

          _ <- updateRequest match {
            case UpdateLabel(label) => accountManagerClient.updateLabel(accountId, label)
            case UpdateSyncFrequency(syncFrequency) =>
              accountManagerClient.updateSyncFrequency(accountId, syncFrequency)
            case UpdateSyncFrequencyAndLabel(syncFrequency, label) =>
              accountManagerClient.updateAccount(accountId, syncFrequency, label)
          }
        } yield ()
        r.flatMap(_ => Ok())

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

      // List account operations
      case GET -> Root / UUIDVar(
            accountId
          ) / "operations" :? OptionalCursorQueryParamMatcher(cursor)
          +& OptionalBoundedLimitQueryParamMatcher(limit)
          +& OptionalSortQueryParamMatcher(sort) =>
        implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
        for {
          boundedLimit <- parseBoundedLimit(limit)
          _            <- log.info(s"Fetching operations for account: $accountId")
          res <- interpreterClient
            .getOperations(
              accountId = accountId,
              limit = boundedLimit.value,
              sort = sort,
              cursor = cursor
            )
            .flatMap(Ok(_))
        } yield res

      // Get operation info
      case GET -> Root / UUIDVar(
            accountId
          ) / "operations" / uid =>
        implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
        log.info(s"Fetching operations for account: $accountId") *>
          interpreterClient
            .getOperation(
              accountId,
              operationId = uid
            )
            .flatMap {
              case Some(operation) => Ok(operation)
              case None            => NotFound(s"No operation in account identified by $uid ")
            }

      // List account UTXOs
      case GET -> Root / UUIDVar(
            accountId
          ) / "utxos" :? OptionalBoundedLimitQueryParamMatcher(limit)
          +& OptionalOffsetQueryParamMatcher(offset)
          +& OptionalSortQueryParamMatcher(sort) =>
        implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
        (for {
          boundedLimit <- parseBoundedLimit(limit)

          _ <- log.info(s"Fetching UTXOs for account: $accountId")

          internalUtxos <- interpreterClient
            .getUtxos(
              accountId = accountId,
              limit = boundedLimit.value,
              offset = offset.getOrElse(0),
              sort = sort
            )
          apiConfirmedUtxos = internalUtxos.utxos.map(apiModels.ConfirmedUtxo.fromCommon)
          response          = apiModels.GetUtxosResult.fromCommon(internalUtxos, apiConfirmedUtxos)
        } yield response).flatMap(Ok(_))

      // Get account balances
      case GET -> Root / UUIDVar(
            accountId
          ) / "balances" :? OptionalStartInstantQueryParamMatcher(start)
          +& OptionalEndInstantQueryParamMatcher(end)
          +& OptionalIntervalQueryParamMatcher(interval) =>
        implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
        log.info(s"Fetching balances history for account: $accountId") *>
          interpreterClient
            .getBalanceHistory(
              accountId,
              start,
              end,
              interval
            )
            .flatMap(Ok(_))

      // Get account balances with preset
      case GET -> Root / UUIDVar(
            accountId
          ) / "balances" / BalancePreset(preset) =>
        implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)
        log.info(s"Fetching balances history for account: $accountId") *>
          interpreterClient
            .getBalanceHistory(
              accountId,
              Some(Instant.now().minus(preset.durationInSeconds, ChronoUnit.SECONDS)),
              Some(Instant.now()),
              Some(preset.interval)
            )
            .flatMap(Ok(_))

      // List observable account addresses
      case GET -> Root / UUIDVar(
            accountId
          ) / "addresses" :? OptionalFromIndexQueryParamMatcher(from)
          +& OptionalToIndexQueryParamMatcher(to)
          +& OptionalChangeTypeParamMatcher(change) =>
        for {
          accountInfo <- accountManagerClient.getAccountInfo(accountId)

          _ <- log.info("Get Observable Addresses")(
            LamaLogContext().withAccount(accountInfo.account)
          )
          keychainId <- UuidUtils.stringToUuidIO(accountInfo.account.identifier)

          response <- keychainClient
            .getAddresses(
              keychainId,
              from.getOrElse(0),
              to.getOrElse(0),
              change
            )
            .flatMap(Ok(_))

        } yield response

      // List fresh account addresses
      case GET -> Root / UUIDVar(
            accountId
          ) / "addresses" / "fresh" :? OptionalChangeTypeParamMatcher(change) =>
        for {
          accountInfo <- accountManagerClient.getAccountInfo(accountId)
          _           <- log.info("Get Fresh Addresses")(LamaLogContext().withAccount(accountInfo.account))

          keychainId   <- UuidUtils.stringToUuidIO(accountInfo.account.identifier)
          keychainInfo <- keychainClient.getKeychainInfo(keychainId)

          response <- keychainClient
            .getFreshAddresses(
              keychainId,
              change.getOrElse(ChangeType.External),
              keychainInfo.lookaheadSize
            )
            .flatMap(Ok(_))

        } yield response

      // Resync account
      case GET -> Root / UUIDVar(accountId) / "resync"
          :? OptionalWipeQueryParamMatcher(wipe) =>
        val followUpId = UUID.randomUUID()
        for {
          accountInfo <- accountManagerClient.getAccountInfo(accountId)
          context = LamaLogContext().withAccount(accountInfo.account).withFollowUpId(followUpId)
          _ <- log.info(s"Fetching account informations for id: $accountId")(context)

          isWipe = wipe.getOrElse(false)

          _ <- log.info(s"Resyncing account $accountId - wipe=$isWipe")(context)

          _ <-
            if (isWipe) {
              for {
                _          <- log.info("Resetting keychain")(context)
                keychainId <- UuidUtils.stringToUuidIO(accountInfo.account.identifier)
                res        <- keychainClient.resetKeychain(keychainId)
                _          <- log.info("Removing interpreter data")(context)
                _ <- interpreterClient.removeDataFromCursor(
                  accountInfo.account.id,
                  None,
                  followUpId
                )
              } yield res
            } else IO.unit

          res <- accountManagerClient.resyncAccount(accountId, isWipe).flatMap(Ok(_))
        } yield res

      // Unregister account
      case DELETE -> Root / UUIDVar(accountId) =>
        for {
          accountInfo <- accountManagerClient.getAccountInfo(accountId)
          context = LamaLogContext().withAccount(accountInfo.account)
          _ <- log.info(s"Fetching account informations for id: $accountId")(context)

          _          <- log.info("Deleting keychain")(context)
          keychainId <- UuidUtils.stringToUuidIO(accountInfo.account.identifier)
          _ <- keychainClient
            .deleteKeychain(keychainId)
            .map(_ => log.info("Keychain deleted")(context))
            .handleErrorWith(_ =>
              log.info("An error occurred while deleting the keychain, moving on")(context)
            )

          _ <- log.info("Unregistering account")(context)
          _ <- accountManagerClient.unregisterAccount(accountInfo.account.id)
          _ <- accountManagerClient.unregisterAccount(accountInfo.account.id)
          _ <- accountManagerClient.unregisterAccount(accountInfo.account.id)
          _ <- log.info("Account unregistered")(context)

          res <- Ok()

        } yield res
    }

}

case class ValidationResult(valid: List[String], invalid: Map[String, String])
object ValidationResult {

  def valid(address: TransactorClient.Address): ValidationResult =
    ValidationResult(valid = List(address.value), invalid = Map.empty)
  def invalid(address: TransactorClient.Address, reason: String): ValidationResult =
    ValidationResult(valid = List.empty, invalid = Map(address.value -> reason))

  implicit val monoid: Monoid[ValidationResult] = new Monoid[ValidationResult] {
    override def empty: ValidationResult = ValidationResult(List.empty, Map.empty)

    override def combine(x: ValidationResult, y: ValidationResult): ValidationResult =
      ValidationResult(
        valid = x.valid ::: y.valid,
        invalid = (x.invalid.toList ::: y.invalid.toList).toMap
      )
  }

}
