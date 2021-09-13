package co.ledger.lama.scheduler

import java.util.UUID
import cats.effect.IO
import co.ledger.lama.scheduler.domain.models.implicits._
import co.ledger.lama.common.utils.IOAssertion
import co.ledger.lama.scheduler.domain.adapters.secondary.queue.RedisPublishingQueue
import co.ledger.lama.scheduler.domain.models.WithBusinessId
import co.ledger.lama.scheduler.domain.services.PublishingQueue
import com.redis.RedisClient
import fs2.Stream
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpecLike
import redis.embedded.RedisServer

import scala.collection.mutable

class PublisherSpec extends AnyFlatSpecLike with Matchers with Inspectors with BeforeAndAfterAll {

  val redisServer              = new RedisServer(6380)
  val redisClient: RedisClient = new RedisClient("localhost", 6380)

  val nbEvents: Int = 10

  def publishers: Seq[TestPublisher] =
    (1 to 5).map(i => new TestPublisher(redisClient, nbEvents, i))

  forAll(publishers) { publisher =>
    val maxOnGoingEvents   = publisher.maxOnGoingEvents
    val events             = publisher.events
    val countPendingEvents = nbEvents - maxOnGoingEvents

    it should s" have $maxOnGoingEvents published events and $countPendingEvents pending events" in IOAssertion {
      Stream
        .emits(events)
        .evalMap(publisher.enqueue)
        .compile
        .drain
        .map { _ =>
          publisher.countPendingEvents shouldBe Some(countPendingEvents)
          publisher.publishedEvents should have size maxOnGoingEvents
          assert(
            publisher.publishedEvents.containsSlice(publisher.events.take(maxOnGoingEvents))
          )
        }
    }
  }

  forAll(publishers) { publisher =>
    val maxOnGoingEvents = publisher.maxOnGoingEvents
    val events           = publisher.events

    it should s"publish messages $maxOnGoingEvents by $maxOnGoingEvents" in IOAssertion {
      Stream
        .emits(events)
        .evalMap(publisher.enqueue)
        .zipWithIndex
        .evalMap { case (_, index) =>
          val publishedEvents   = publisher.publishedEvents
          val pendingEventsSize = publisher.countPendingEvents

          // at each iteration, call dequeue to publish next event
          publisher.dequeue(publisher.key).map { _ =>
            (index, pendingEventsSize, publishedEvents)
          }
        }
        .map { case (index, pendingEventsSize, publishedEvents) =>
          pendingEventsSize shouldBe Some(0)
          publishedEvents shouldBe publisher.events.take(index.toInt + 1)
        }
        .compile
        .drain
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    redisServer.start()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    redisServer.stop()
  }

}

case class TestEvent(accountId: UUID, eventId: String) extends WithBusinessId[UUID] {
  val businessId: UUID = accountId
}

object TestEvent {
  implicit val decoder: Decoder[TestEvent] = deriveConfiguredDecoder[TestEvent]
  implicit val encoder: Encoder[TestEvent] = deriveConfiguredEncoder[TestEvent]
}

class TestPublisher(
    val redis: RedisClient,
    val nbEvents: Int,
    val maxOnGoingEvents: Int
)(implicit
    val enc: Encoder[TestEvent],
    val dec: Decoder[TestEvent]
) extends PublishingQueue[UUID, TestEvent] {
  import co.ledger.lama.scheduler.domain.services.PublishingQueue._

  val delegate = new RedisPublishingQueue[UUID, TestEvent](publish, redis, maxOnGoingEvents)

  val key: UUID = UUID.randomUUID()

  val events: Seq[TestEvent] = (1 to nbEvents).map(i => TestEvent(key, s"event$i"))

  var publishedEvents: mutable.Seq[TestEvent] = mutable.Seq.empty

  def countPendingEvents: Option[Long] = redis.llen(pendingEventsKey(key.toString))

  override def publish = (event: TestEvent) =>
    IO.pure {
      publishedEvents = publishedEvents ++ Seq(event)
    }

  override def enqueue(e: TestEvent): IO[Unit] = delegate.enqueue(e)

  override def dequeue(key: UUID): IO[Unit] = delegate.dequeue(key)
}
