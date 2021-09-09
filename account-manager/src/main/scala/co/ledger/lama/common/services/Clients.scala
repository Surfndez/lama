package co.ledger.lama.common.services

import java.util.concurrent.Executors
import cats.effect.{ConcurrentEffect, ContextShift, IO, Resource, Timer}
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder

import scala.concurrent.ExecutionContext

object Clients {

  def rabbit(
      conf: Fs2RabbitConfig
  )(implicit cs: ContextShift[IO], t: Timer[IO]): Resource[IO, RabbitClient[IO]] =
    RabbitUtils.createClient(conf)

  def htt4s(implicit ce: ConcurrentEffect[IO]): Resource[IO, Client[IO]] =
    BlazeClientBuilder[IO](
      ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)
    ).resource

}
