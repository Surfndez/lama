package co.ledger.lama.common.services

import cats.effect.{IO, Resource}
import co.ledger.lama.common.utils.rabbitmq.RabbitUtils
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

object Clients {

  def rabbit(
      conf: Fs2RabbitConfig
  ): Resource[IO, RabbitClient[IO]] =
    RabbitUtils.createClient(conf)

  def htt4s: Resource[IO, Client[IO]] =
    BlazeClientBuilder[IO](
      ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)
    ).resource

}
