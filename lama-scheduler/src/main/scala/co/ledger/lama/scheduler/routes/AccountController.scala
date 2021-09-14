package co.ledger.lama.scheduler.routes

import cats.effect.IO
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

object AccountController extends Http4sDsl[IO] {

  def accountRoutes(): HttpRoutes[IO] = HttpRoutes.of[IO] { case _ @POST -> Root / "accounts" =>
    Ok()

  }

}
