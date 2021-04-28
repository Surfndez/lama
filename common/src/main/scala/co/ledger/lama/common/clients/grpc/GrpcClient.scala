package co.ledger.lama.common.clients.grpc

import cats.effect.std.Dispatcher
import co.ledger.lama.common.Exceptions.GrpcClientException
import co.ledger.lama.common.logging.DefaultContextLogging
import io.grpc.{CallOptions, ManagedChannel, StatusRuntimeException}

object GrpcClient extends DefaultContextLogging {
  type Builder[F[_], Client] =
    (
        Dispatcher[F],
        ManagedChannel,
        CallOptions,
        StatusRuntimeException => Option[GrpcClientException]
    ) => Client

  private def onError(
      clientName: String
  )(e: StatusRuntimeException): Option[GrpcClientException] =
    Some(GrpcClientException(e, clientName))

  def resolveClient[F[_], Client](
      f: Builder[F, Client],
      dispatcher: Dispatcher[F],
      managedChannel: ManagedChannel,
      clientName: String
  ): Client = f(dispatcher, managedChannel, CallOptions.DEFAULT, onError(clientName))
}
