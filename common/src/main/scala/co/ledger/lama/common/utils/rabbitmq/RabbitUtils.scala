package co.ledger.lama.common.utils.rabbitmq

import cats.data.Kleisli
import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import cats.effect.{IO, Resource}
import cats.implicits._
import co.ledger.lama.common.logging.DefaultContextLogging
import co.ledger.lama.common.utils.ResourceUtils.retriableResource
import co.ledger.lama.common.utils.RetryPolicy
import dev.profunktor.fs2rabbit.config.{Fs2RabbitConfig, deletion}
import dev.profunktor.fs2rabbit.config.declaration.DeclarationQueueConfig
import dev.profunktor.fs2rabbit.config.deletion.{DeletionExchangeConfig, DeletionQueueConfig}
import dev.profunktor.fs2rabbit.effects.MessageEncoder
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model._
import fs2.Stream
import io.circe.{Decoder, Encoder}
import io.circe.parser.parse
import io.circe.syntax._

import java.nio.charset.StandardCharsets

object RabbitUtils extends DefaultContextLogging {
  def createClient(
      conf: Fs2RabbitConfig
  ): Resource[IO, RabbitClient[IO]] = {
    for {
      client <- RabbitClient.resource[IO](conf)

      _ = log.logger.info("Creating rabbitmq client")

      _ <- retriableResource(
        "Create rabbitmq client",
        client.createConnectionChannel,
        RetryPolicy.exponential()
      )

      _ = log.logger.info("Rabbitmq client created")
    } yield client
  }

  def declareExchanges(
      R: RabbitClient[IO],
      exchanges: List[(ExchangeName, ExchangeType)]
  ): IO[Unit] =
    R.createConnectionChannel
      .use { implicit channel =>
        exchanges
          .map { case (exchangeName, exchangeType) =>
            R.declareExchange(exchangeName, exchangeType)
          }
          .sequence
          .void
      }

  def declareBindings(
      R: RabbitClient[IO],
      bindings: List[(ExchangeName, RoutingKey, QueueName)]
  ): IO[Unit] =
    R.createConnectionChannel.use { implicit channel =>
      bindings
        .map { case (exchangeName, routingKey, queueName) =>
          R.declareQueue(DeclarationQueueConfig.default(queueName)) *>
            R.bindQueue(queueName, exchangeName, routingKey)
        }
        .sequence
        .void
    }

  def deleteBindings(R: RabbitClient[IO], queues: List[QueueName]): IO[Unit] =
    Stream
      .resource(R.createConnectionChannel)
      .flatMap { implicit channel =>
        Stream
          .emits(queues)
          .map { queueName =>
            Stream.eval {
              R.deleteQueue(
                DeletionQueueConfig(
                  queueName,
                  deletion.Used,
                  deletion.NonEmpty
                )
              )
            }
          }
          .parJoinUnbounded
      }
      .compile
      .drain

  def deleteExchanges(R: RabbitClient[IO], exchanges: List[ExchangeName]): IO[Unit] =
    Stream
      .resource(R.createConnectionChannel)
      .flatMap { implicit channel =>
        Stream
          .emits(exchanges)
          .map { exchangeName =>
            Stream.eval {
              R.deleteExchange(
                DeletionExchangeConfig(exchangeName, deletion.Used)
              )
            }
          }
          .parJoinUnbounded
      }
      .compile
      .drain

  def createAutoAckConsumer[A](
      R: RabbitClient[IO],
      queueName: QueueName
  )(implicit d: Decoder[A]): Stream[IO, A] =
    Stream
      .resource(R.createConnectionChannel)
      .evalMap { implicit channel =>
        R.createAutoAckConsumer[String](queueName)
      }
      .flatten
      .evalMap { message =>
        val parsed = parse(message.payload).flatMap(_.as[A])
        IO.fromEither(parsed)
      }

  def consume[A: Decoder](
      R: RabbitClient[IO],
      queueName: QueueName
  )(implicit channel: AMQPChannel): IO[Stream[IO, A]] =
    R.createAutoAckConsumer[String](queueName).map { queue =>
      queue.evalMap(message => IO.fromEither(io.circe.parser.decode(message.payload)))
    }

  def createConsumer[A](R: RabbitClient[IO], queueName: QueueName)(implicit
      d: Decoder[A]
  ): Stream[IO, AutoAckMessage[A]] = {
    Stream
      .resource(R.createConnectionChannel)
      .evalMap { implicit channel =>
        R.createAckerConsumer[String](queueName)
      }
      .flatMap { case (acker, queue) =>
        queue.evalMap { message =>
          parse(message.payload)
            .flatMap(_.as[A])
            .liftTo[IO]
            .flatMap(AutoAckMessage.wrap(_, message.deliveryTag)(acker))
            .guaranteeCase {
              case Canceled() | Errored(_) => acker(AckResult.NAck(message.deliveryTag))
              case Succeeded(_)            => IO.unit
            }
        }
      }
  }

  def createPublisher[A](
      R: RabbitClient[IO],
      exchangeName: ExchangeName,
      routingKey: RoutingKey
  )(implicit e: Encoder[A]): Stream[IO, A => IO[Unit]] = {
    implicit val me: MessageEncoder[IO, A] =
      Kleisli[IO, A, AmqpMessage[Array[Byte]]] { s =>
        AmqpMessage(
          payload = s.asJson.noSpaces.getBytes(StandardCharsets.UTF_8),
          properties = AmqpProperties.empty
        ).pure[IO]
      }

    Stream
      .resource(R.createConnectionChannel)
      .evalMap { implicit channel =>
        R.createPublisher[A](
          exchangeName,
          routingKey
        )
      }
  }

}
