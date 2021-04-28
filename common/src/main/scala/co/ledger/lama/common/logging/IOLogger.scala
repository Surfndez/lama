package co.ledger.lama.common.logging

import cats.effect.IO
import com.typesafe.scalalogging.LoggerTakingImplicit

case class IOLogger(logger: LoggerTakingImplicit[LogContext]) {

  // TRACE

  def trace(message: String)(implicit lc: LogContext): IO[Unit] =
    IO(logger.trace(message)).attempt.void

  def trace(message: String, cause: Throwable)(implicit lc: LogContext): IO[Unit] =
    IO(logger.trace(message, cause)).attempt.void

  // DEBUG

  def debug(message: => String)(implicit lc: LogContext): IO[Unit] =
    IO(logger.debug(message)).attempt.void

  def debug(message: => String, cause: Throwable)(implicit lc: LogContext): IO[Unit] =
    IO(logger.debug(message, cause)).attempt.void

  // INFO

  def info(message: String)(implicit lc: LogContext): IO[Unit] =
    IO(logger.info(message)).attempt.void

  def info(message: String, cause: Throwable)(implicit lc: LogContext): IO[Unit] =
    IO(logger.info(message, cause)).attempt.void

  // WARN

  def warn(message: String)(implicit lc: LogContext): IO[Unit] =
    IO(logger.warn(message)).attempt.void

  def warn(message: String, cause: Throwable)(implicit lc: LogContext): IO[Unit] =
    IO(logger.warn(message, cause)).attempt.void

  // ERROR

  def error(message: String)(implicit lc: LogContext): IO[Unit] =
    IO(logger.error(message)).attempt.void

  def error(message: String, cause: Throwable)(implicit lc: LogContext): IO[Unit] =
    IO(logger.error(message, cause)).attempt.void

}
