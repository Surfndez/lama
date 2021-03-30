package co.ledger.lama.bitcoin.api.utils

import cats.data.ValidatedNel
import cats.effect.IO

import java.time.Instant
import java.time.format.DateTimeFormatter
import co.ledger.lama.bitcoin.common.models.interpreter.ChangeType
import co.ledger.lama.common.models.Sort
import org.http4s.{ParseFailure, QueryParamCodec, QueryParamDecoder}
import org.http4s.dsl.io.{
  OptionalQueryParamDecoderMatcher,
  OptionalValidatingQueryParamDecoderMatcher
}

object RouterUtils {
  implicit val sortQueryParamDecoder: QueryParamDecoder[Sort] =
    QueryParamDecoder[String].map(Sort.fromKey(_).getOrElse(Sort.Descending))

  object OptionalBlockHeightQueryParamMatcher
      extends OptionalQueryParamDecoderMatcher[Long]("blockHeight")

  private val maxLimit: Int = 1000

  class BoundedLimit(val value: Int) extends AnyVal

  implicit val boundedLimitQueryParamMatcher: QueryParamDecoder[BoundedLimit] =
    QueryParamDecoder[Int].emap { limit =>
      Either.cond(
        limit <= maxLimit,
        new BoundedLimit(limit),
        ParseFailure(s"limit param should be <= $maxLimit", limit.toString)
      )
    }

  object OptionalBoundedLimitQueryParamMatcher
      extends OptionalValidatingQueryParamDecoderMatcher[BoundedLimit]("limit")

  def parseBoundedLimit(
      queryParam: Option[ValidatedNel[ParseFailure, BoundedLimit]]
  ): IO[BoundedLimit] = {
    queryParam match {
      case None               => IO.pure(new BoundedLimit(maxLimit))
      case Some(boundedLimit) => boundedLimit.fold(e => IO.raiseError(e.head), IO.pure)
    }
  }

  object OptionalOffsetQueryParamMatcher extends OptionalQueryParamDecoderMatcher[Int]("offset")
  object OptionalSortQueryParamMatcher   extends OptionalQueryParamDecoderMatcher[Sort]("sort")

  implicit val isoInstantCodec: QueryParamCodec[Instant] =
    QueryParamCodec.instantQueryParamCodec(DateTimeFormatter.ISO_INSTANT)

  object OptionalStartInstantQueryParamMatcher
      extends OptionalQueryParamDecoderMatcher[Instant]("start")

  object OptionalEndInstantQueryParamMatcher
      extends OptionalQueryParamDecoderMatcher[Instant]("end")

  object OptionalIntervalQueryParamMatcher extends OptionalQueryParamDecoderMatcher[Int]("interval")

  object OptionalFromIndexQueryParamMatcher extends OptionalQueryParamDecoderMatcher[Int]("from")

  object OptionalToIndexQueryParamMatcher extends OptionalQueryParamDecoderMatcher[Int]("to")

  implicit val changeQueryParamDecoder: QueryParamDecoder[ChangeType] =
    QueryParamDecoder[String].emap {
      case "external" => Right(ChangeType.External)
      case "internal" => Right(ChangeType.Internal)
      case unknown    => Left(ParseFailure("Unknown change type", unknown))
    }

  object OptionalChangeTypeParamMatcher
      extends OptionalQueryParamDecoderMatcher[ChangeType]("change")

  object OptionalWipeQueryParamMatcher extends OptionalQueryParamDecoderMatcher[Boolean]("wipe")

}
