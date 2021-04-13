package co.ledger.lama.common.utils

import cats.effect.{IO, Timer}
import fs2.{Chunk, Pull, Stream}

object IOUtils {

  def fetchPaginatedItems[T](
      evaluate: (Int, Int) => IO[T],
      continue: T => Boolean,
      offset: Int = 0,
      limit: Int = 20
  ): Pull[IO, T, Unit] = {
    Pull
      .eval(
        evaluate(offset, limit)
      )
      .flatMap { res =>
        if (continue(res)) {
          Pull.output(Chunk(res)) >>
            fetchPaginatedItems[T](evaluate, continue, offset + limit, limit)
        } else {
          Pull.output(Chunk(res))
        }
      }
  }

  def fetchPaginatedItemsByCursor[T](
      evaluate: (Int, Option[String]) => IO[T],
      getNextCursor: T => Option[String],
      limit: Int = 20,
      cursor: Option[String]
  ): Pull[IO, T, Unit] = {
    Pull
      .eval(
        evaluate(limit, cursor)
      )
      .flatMap { res =>
        getNextCursor(res) match {
          case nextCursor @ Some(_) =>
            Pull.output(Chunk(res)) >>
              fetchPaginatedItemsByCursor[T](evaluate, getNextCursor, limit, nextCursor)

          case None =>
            Pull.output(Chunk(res))
        }
      }
  }

  def retry[T](io: IO[T], policy: RetryPolicy = RetryPolicy.linear())(implicit
      t: Timer[IO]
  ): IO[T] = {
    Stream
      .eval(io)
      .attempts(policy)
      .collectFirst { case Right(res) =>
        res
      }
      .compile
      .lastOrError
  }

  def retryIf[T](io: IO[T], success: T => Boolean, policy: RetryPolicy = RetryPolicy.linear())(
      implicit t: Timer[IO]
  ): IO[T] =
    retry(
      io.flatMap { res =>
        if (success(res))
          IO.pure(res)
        else
          IO.raiseError(new Exception())
      },
      policy
    )
}
