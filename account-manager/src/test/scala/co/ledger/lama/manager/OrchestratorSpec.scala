package co.ledger.lama.manager

import java.time.Instant
import java.util.UUID
import cats.effect.IO
import co.ledger.lama.common.models._
import co.ledger.lama.common.utils.IOAssertion
import co.ledger.lama.common.utils.rabbitmq.AutoAckMessage
import dev.profunktor.fs2rabbit.model.DeliveryTag
import fs2.{Pipe, Stream}
import io.circe.JsonObject
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.concurrent.duration._

class OrchestratorSpec extends AnyFlatSpecLike with Matchers {

  it should "succeed" in IOAssertion {
    val nbAccounts: Int            = 10
    val takeNbElements: Int        = 3
    val awakeEvery: FiniteDuration = 0.5.seconds
    val orchestrator               = new FakeOrchestrator(nbAccounts, awakeEvery)

    orchestrator.run(Some(takeNbElements)).compile.drain.map { _ =>
      orchestrator.tasks.foreach { t =>
        t.publishedWorkerEvents.keys should have size nbAccounts
        t.publishedWorkerEvents.values.foreach(_ should have size takeNbElements)
        t.reportedEvents should have size nbAccounts
        t.triggeredEvents should have size takeNbElements * nbAccounts
      }
    }
  }

}

class FakeOrchestrator(nbEvents: Int, override val awakeEvery: FiniteDuration)
    extends Orchestrator {

  val workerEvents: Seq[WorkableEvent[JsonObject]] = {
    val now = Instant.now()

    (1 to nbEvents).map { i =>
      WorkableEvent[JsonObject](
        account = Account(s"xpub-$i", CoinFamily.Bitcoin, Coin.Btc, AccountGroup("TestGroup")),
        syncId = UUID.randomUUID(),
        status = Status.Registered,
        cursor = None,
        error = None,
        time = now
      )
    }
  }

  val tasks: List[FakeSyncEventTask] = List(new FakeSyncEventTask(workerEvents))

}

class FakeSyncEventTask(workerEvents: Seq[WorkableEvent[JsonObject]]) extends SyncEventTask {

  var reportedEvents: mutable.Seq[ReportableEvent[JsonObject]] = mutable.Seq.empty

  var publishedWorkerEvents: mutable.Map[UUID, List[WorkableEvent[JsonObject]]] =
    mutable.Map.empty

  var triggeredEvents: mutable.Seq[SyncEvent[JsonObject]] = mutable.Seq.empty

  def publishableWorkerEvents: Stream[IO, WorkableEvent[JsonObject]] =
    Stream.emits(workerEvents)

  def publishWorkerEventsPipe: Pipe[IO, WorkableEvent[JsonObject], Unit] =
    _.evalMap { event =>
      IO(
        publishedWorkerEvents.update(
          event.account.id,
          publishedWorkerEvents.getOrElse(event.account.id, List.empty) :+ event
        )
      )
    }

  def reportableEvents: Stream[IO, AutoAckMessage[ReportableEvent[JsonObject]]] =
    Stream
      .emits(workerEvents)
      .evalMap(event =>
        AutoAckMessage.wrap(
          event.asReportableSuccessEvent(None),
          DeliveryTag(0L)
        )(_ => IO.unit)
      )

  def reportEventPipe: Pipe[IO, AutoAckMessage[ReportableEvent[JsonObject]], Unit] =
    _.evalMap {
      _.unwrap { event =>
        IO { reportedEvents = reportedEvents :+ event }
      }
    }

  def triggerableEvents: Stream[IO, TriggerableEvent[JsonObject]] =
    Stream.emits(
      workerEvents.map(event =>
        TriggerableEvent(
          event.account,
          event.syncId,
          Status.Synchronized,
          event.cursor,
          event.error,
          Instant.now()
        )
      )
    )

  def triggerEventsPipe: Pipe[IO, TriggerableEvent[JsonObject], Unit] =
    _.evalMap { e =>
      IO { triggeredEvents = triggeredEvents :+ e.nextWorkable }
    }
}
