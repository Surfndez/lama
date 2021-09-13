package co.ledger.lama.scheduler.domain.services

import cats.effect.IO
import co.ledger.lama.scheduler.domain.models.WithBusinessId

/** Publisher publishing events sequentially.
  * Redis is used as a FIFO queue to guarantee the sequence.
  */
trait PublishingQueue[K, V <: WithBusinessId[K]] {
  // The inner publish function.
  def publish: V => IO[Unit]

  // If the counter of ongoing events for the key has reached max ongoing events, add the event to the pending list.
  // Otherwise, publish and increment the counter of ongoing events.
  def enqueue(e: V): IO[Unit]

  // Remove the top pending event of a key and take the next pending event.
  // If next pending event exists, publish it.
  def dequeue(key: K): IO[Unit]
}

object PublishingQueue {
  // Stored keys.
  def onGoingEventsCounterKey[K](key: K): String = s"on_going_events_counter_$key"
  def pendingEventsKey[K](key: K): String        = s"pending_events_$key"
}