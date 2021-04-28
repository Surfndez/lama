package co.ledger.lama.common.utils.rabbitmq

import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Outcome.Succeeded
import dev.profunktor.fs2rabbit.model.{AckResult, DeliveryTag}

/** Wraps data which has to be acknowledged (or not)
  * @tparam A data
  */
sealed trait AutoAckMessage[A] {
  private[rabbitmq] val message: A

  def unwrap[B](f: A => IO[B]): IO[B] = {
    f(message)
      .guaranteeCase {
        case Succeeded(_) => ack()
        case _            => nack()
      }
  }

  def ack(): IO[Unit]
  def nack(): IO[Unit]
}

final private class RabbitMQAutoAckMessage[A](
    val message: A,
    private val deliveryTag: DeliveryTag,
    private val acker: AckResult => IO[Unit]
) extends AutoAckMessage[A] {
  def ack(): IO[Unit]  = acker(AckResult.Ack(deliveryTag))
  def nack(): IO[Unit] = acker(AckResult.NAck(deliveryTag))
}

object AutoAckMessage {
  def wrap[A](message: A, deliveryTag: DeliveryTag)(
      acker: AckResult => IO[Unit]
  ): IO[AutoAckMessage[A]] =
    Ref
      .of[IO, Boolean](false)
      .map { isAcked =>
        def wrappedAcker(result: AckResult): IO[Unit] = for {
          acked <- isAcked.getAndSet(true)
          res   <- if (acked) IO.unit else acker(result)
        } yield res

        new RabbitMQAutoAckMessage(message, deliveryTag, wrappedAcker)
      }
}
