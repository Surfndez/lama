package co.ledger.lama.bitcoin.api.routes

import cats.effect.IO

import co.ledger.lama.bitcoin.api.models.transactor.GenerateSignaturesRequest
import co.ledger.lama.bitcoin.common.clients.grpc.TransactorClient
import co.ledger.lama.common.logging.{ContextLogging, LamaLogContext}

import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.circe.CirceEntityCodec._

object InternalsController extends Http4sDsl[IO] with ContextLogging {

  def internalRoutes(
      transactorClient: TransactorClient
  ): HttpRoutes[IO] = HttpRoutes.of[IO] {

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

  }
}
