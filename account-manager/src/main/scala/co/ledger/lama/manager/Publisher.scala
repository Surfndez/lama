package co.ledger.lama.manager

import java.util.UUID
import cats.effect.IO
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import co.ledger.lama.common.models.{WithBusinessId, WorkableEvent}
import co.ledger.lama.manager.Exceptions.RedisUnexpectedException
import com.redis.RedisClient
import com.redis.serialization.{Format, Parse}
import com.redis.serialization.Parse.Implicits._
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{ExchangeName, RoutingKey}
import fs2.Stream
import io.circe.{Decoder, Encoder, JsonObject}
import io.circe.parser.decode
import io.circe.syntax._

import scala.annotation.nowarn

/** Publisher publishing events sequentially.
  * Redis is used as a FIFO queue to guarantee the sequence.
  */
trait Publisher[K, V <: WithBusinessId[K]] {
  import Publisher._

  // Max concurrent ongoing events.
  val maxOnGoingEvents: Int = 1

  // Redis client.
  def redis: RedisClient

  // The inner publish function.
  def publish(event: V): IO[Unit]

  // Implicits for serializing data as json and storing it as binary in redis.
  implicit val dec: Decoder[V]
  implicit val enc: Encoder[V]

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

  // If the counter of ongoing events for the key has reached max ongoing events, add the event to the pending list.
  // Otherwise, publish and increment the counter of ongoing events.
  def enqueue(e: V): IO[Unit] =
    hasMaxOnGoingMessages(e.businessId).flatMap {
      case true =>
        // enqueue pending events in redis
        rpushPendingMessages(e)
      case false =>
        // publish and increment the counter of ongoing events
        publish(e)
          .flatMap(_ => incrOnGoingMessages(e.businessId))
    }.void

  // Remove the top pending event of a key and take the next pending event.
  // If next pending event exists, publish it.
  def dequeue(key: K): IO[Unit] =
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

object Publisher {
  // Stored keys.
  def onGoingEventsCounterKey[K](key: K): String = s"on_going_events_counter_$key"
  def pendingEventsKey[K](key: K): String        = s"pending_events_$key"
}

class WorkableEventPublisher(
    val redis: RedisClient,
    rabbit: RabbitClient[IO],
    exchangeName: ExchangeName,
    routingKey: RoutingKey
)(implicit val enc: Encoder[WorkableEvent[JsonObject]], val dec: Decoder[WorkableEvent[JsonObject]])
    extends Publisher[UUID, WorkableEvent[JsonObject]]
    with DefaultContextLogging {

  def publish(event: WorkableEvent[JsonObject]): IO[Unit] =
    publisher
      .evalMap(p => p(event) *> log.info(s"Published event to worker: ${event.asJson.toString}"))
      .compile
      .drain

  private val publisher: Stream[IO, WorkableEvent[JsonObject] => IO[Unit]] =
    RabbitUtils.createPublisher[WorkableEvent[JsonObject]](
      rabbit,
      exchangeName,
      routingKey
    )

}
