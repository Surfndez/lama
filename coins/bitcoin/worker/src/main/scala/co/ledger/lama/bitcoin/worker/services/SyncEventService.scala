package co.ledger.lama.bitcoin.worker.services

import cats.effect.IO
import co.ledger.lama.bitcoin.common.models.explorer.Block
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.common.utils.rabbitmq.{AutoAckMessage, RabbitUtils}
import co.ledger.lama.common.models.{ReportableEvent, WorkableEvent}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{ExchangeName, QueueName, RoutingKey}
import fs2.Stream
import io.circe.syntax._

trait SyncEventService {
  def consumeWorkerEvents: Stream[IO, AutoAckMessage[WorkableEvent[Block]]]
  def reportEvent(message: ReportableEvent[Block]): IO[Unit]
}

class RabbitSyncEventService(
    rabbitClient: RabbitClient[IO],
    workerQueueName: QueueName,
    lamaExchangeName: ExchangeName,
    lamaRoutingKey: RoutingKey
) extends SyncEventService
    with DefaultContextLogging {

  def consumeWorkerEvents: Stream[IO, AutoAckMessage[WorkableEvent[Block]]] =
    RabbitUtils.createConsumer[WorkableEvent[Block]](rabbitClient, workerQueueName)

  private val publisher: Stream[IO, ReportableEvent[Block] => IO[Unit]] =
    RabbitUtils
      .createPublisher[ReportableEvent[Block]](rabbitClient, lamaExchangeName, lamaRoutingKey)

  def reportEvent(message: ReportableEvent[Block]): IO[Unit] =
    publisher
      .evalMap(p => p(message) *> log.info(s"Published message: ${message.asJson.toString}"))
      .compile
      .drain

}
