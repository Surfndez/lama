package co.ledger.lama.common.utils

import cats.effect.IO

object IOAssertion {
  import cats.effect.unsafe.implicits.global

  def apply[A](ioa: IO[A]): A = ioa.unsafeRunSync()
}
