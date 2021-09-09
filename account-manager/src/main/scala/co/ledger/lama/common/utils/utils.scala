package co.ledger.lama.common

import fs2.{Pure, Stream}

import scala.concurrent.duration._

package object utils {

  type RetryPolicy = Stream[Pure, FiniteDuration]

  object RetryPolicy {
    def linear(delay: FiniteDuration = 1.second, maxRetry: Int = 20): RetryPolicy =
      Stream.emit(delay).repeatN(maxRetry)

    def exponential(
        initial: FiniteDuration = 50.millisecond,
        factor: Long = 2,
        maxElapsedTime: FiniteDuration = 2.minute
    ): RetryPolicy = Stream.iterate(initial)(_ * factor).takeWhile(_ < maxElapsedTime)
  }

}
