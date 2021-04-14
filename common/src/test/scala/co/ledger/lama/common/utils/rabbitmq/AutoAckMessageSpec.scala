package co.ledger.lama.common.utils.rabbitmq

import cats.effect.IO
import cats.effect.concurrent.Ref
import co.ledger.lama.common.utils.IOAssertion
import dev.profunktor.fs2rabbit.model.{AckResult, DeliveryTag}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AutoAckMessageSpec extends AnyFunSuite with Matchers {
  case class EmptyMessage()

  case class VerifiableAcker(callsCounter: Ref[IO, Int], acker: AckResult => IO[Unit])

  def genVerifiableAcker: IO[VerifiableAcker] = {
    Ref
      .of[IO, Int](0)
      .map { callsCounter =>
        VerifiableAcker(
          callsCounter,
          _ =>
            for {
              counter <- callsCounter.get
              res <-
                if (counter != 0) IO.raiseError(new Exception("Acker was already called!"))
                else callsCounter.update(_ + 1)
            } yield res
        )
      }
  }

  test("Acker should be called only once") {
    IOAssertion {
      val deliveryTag = DeliveryTag(1L)
      for {
        verifiableAcker <- genVerifiableAcker
        ackMessage      <- AutoAckMessage.wrap(EmptyMessage(), deliveryTag)(verifiableAcker.acker)
        _               <- ackMessage.unwrap(_ => IO.unit)
        _               <- ackMessage.unwrap(_ => IO.unit)
        counter         <- verifiableAcker.callsCounter.get
      } yield assert(counter == 1)
    }
  }

  test("Acker should be called only once when using ack inside unwrap") {
    IOAssertion {
      val deliveryTag = DeliveryTag(1L)
      for {
        verifiableAcker <- genVerifiableAcker
        ackMessage      <- AutoAckMessage.wrap(EmptyMessage(), deliveryTag)(verifiableAcker.acker)
        _               <- ackMessage.unwrap(_ => ackMessage.ack() *> IO.unit)
        counter         <- verifiableAcker.callsCounter.get
      } yield assert(counter == 1)
    }
  }

  test("Acker should be called only once when using nack inside unwrap") {
    IOAssertion {
      val deliveryTag = DeliveryTag(1L)
      for {
        verifiableAcker <- genVerifiableAcker
        ackMessage      <- AutoAckMessage.wrap(EmptyMessage(), deliveryTag)(verifiableAcker.acker)
        _               <- ackMessage.unwrap(_ => ackMessage.nack() *> IO.unit)
        counter         <- verifiableAcker.callsCounter.get
      } yield assert(counter == 1)
    }
  }
}
