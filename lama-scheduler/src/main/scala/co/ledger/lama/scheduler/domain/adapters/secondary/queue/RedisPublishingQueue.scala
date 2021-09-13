package co.ledger.lama.scheduler.domain.adapters.secondary.queue

import cats.effect.IO
import co.ledger.lama.scheduler.Exceptions.RedisUnexpectedException
import co.ledger.lama.scheduler.domain.models.WithBusinessId
import co.ledger.lama.scheduler.domain.services.PublishingQueue
import co.ledger.lama.scheduler.domain.services.PublishingQueue.{onGoingEventsCounterKey, pendingEventsKey}
import com.redis.RedisClient
import io.circe.parser.decode
import com.redis.serialization.Parse.Implicits._
import io.circe.syntax._
import com.redis.serialization.{Format, Parse}
import io.circe.{Decoder, Encoder}

import scala.annotation.nowarn

final class RedisPublishingQueue[K, V <: WithBusinessId[K]](
                                                             override val publish: V => IO[Unit],
                                                             redis: RedisClient,
                                                             val maxOnGoingEvents: Int = 1
                                                           )(implicit val encoder: Encoder[V], decoder: Decoder[V])
  extends PublishingQueue[K, V] {

  implicit val parse: Parse[V] =
    Parse { bytes =>
      decode[V](new String(bytes)) match {
        case Right(v) => v
        case Left(e)  => throw e
      }
    }

  @nowarn
  implicit val fmt: Format =
    Format { case v: V =>
      v.asJson.noSpaces.getBytes()
    }

  override def enqueue(e: V): IO[Unit] =
    hasMaxOnGoingMessages(e.businessId).flatMap {
      case true =>
        // enqueue pending events in redis
        rpushPendingMessages(e)
      case false =>
        // publish and increment the counter of ongoing events
        publish(e)
          .flatMap(_ => incrOnGoingMessages(e.businessId))
    }.void

  override def dequeue(key: K): IO[Unit] =
    for {
      _         <- decrOnGoingMessages(key)
      nextEvent <- lpopPendingMessages(key)
      result <- nextEvent match {
        case Some(next) =>
          publish(next)
        case None => IO.unit
      }
    } yield result

  // Count the number of ongoing events
  private def countOnGoingMessages(key: K): IO[Long] =
    IO(redis.get[Long](onGoingEventsCounterKey(key)).getOrElse(0))

  // Check if the counter of ongoing events has reached the max.
  private def hasMaxOnGoingMessages(key: K): IO[Boolean] =
    countOnGoingMessages(key).map(_ >= maxOnGoingEvents)

  // https://redis.io/commands/incr
  // Increment the counter of ongoing events for a key and return the value after.
  private def incrOnGoingMessages(key: K): IO[Long] =
    IO.fromOption(redis.incr(onGoingEventsCounterKey(key)))(RedisUnexpectedException)

  // https://redis.io/commands/decr
  // If the counter is above 0,
  // decrement the counter of ongoing events for a key and return the value after.
  private def decrOnGoingMessages(key: K): IO[Long] =
    countOnGoingMessages(key).flatMap {
      case 0 => IO.pure(0)
      case _ => IO.fromOption(redis.decr(onGoingEventsCounterKey(key)))(RedisUnexpectedException)
    }

  // https://redis.io/commands/rpush
  // Add an event at the last and return the length after.
  private def rpushPendingMessages(event: V): IO[Long] =
    IO.fromOption(redis.rpush(pendingEventsKey(event.businessId), event))(
      RedisUnexpectedException
    )

  // https://redis.io/commands/lpop
  // Remove the first from a key and if exists, return the next one.
  private def lpopPendingMessages(key: K): IO[Option[V]] =
    IO(redis.lpop[V](pendingEventsKey(key)))
}
