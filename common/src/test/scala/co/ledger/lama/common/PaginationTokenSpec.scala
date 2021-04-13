package co.ledger.lama.common

import co.ledger.lama.common.models.PaginationToken
import co.ledger.lama.common.models.implicits._
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PaginationTokenSpec extends AnyFunSuite with Matchers {

  case class TestState(a: String, b: Int)

  object TestState {
    implicit val encoder: Encoder[TestState] =
      deriveConfiguredEncoder[TestState]

    implicit val decoder: Decoder[TestState] =
      deriveConfiguredDecoder[TestState]
  }

  test("encode and decode pagination token") {
    val paginationToken = PaginationToken(TestState("lama", 123), isNext = true)

    val base64PaginationToken  = paginationToken.toBase64
    val decodedPaginationToken = PaginationToken.fromBase64[TestState](base64PaginationToken)

    assert(decodedPaginationToken.contains(paginationToken))
  }
}
