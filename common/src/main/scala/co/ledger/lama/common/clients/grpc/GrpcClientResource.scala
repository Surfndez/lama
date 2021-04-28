package co.ledger.lama.common.clients.grpc

import cats.effect.IO
import cats.effect.std.Dispatcher
import io.grpc.ManagedChannel

final case class GrpcClientResource(dispatcher: Dispatcher[IO], channel: ManagedChannel)
