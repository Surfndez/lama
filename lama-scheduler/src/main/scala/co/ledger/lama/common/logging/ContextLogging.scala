package co.ledger.lama.common.logging

import com.typesafe.scalalogging.{CanLog, Logger, LoggerTakingImplicit}
import org.slf4j.MDC

trait ContextLogging {

  implicit case object CanLogContext extends CanLog[LogContext] {
    override def logMessage(originalMsg: String, lc: LogContext): String = {
      lc.asMap().foreach { case (key, value) => MDC.put(key, value) }
      originalMsg
    }

    override def afterLog(lc: LogContext): Unit = {
      lc.asMap().keys.foreach(MDC.remove)
    }
  }

  val logger: LoggerTakingImplicit[LogContext] = Logger.takingImplicit[LogContext]("LamaLogger")
  val log: IOLogger                            = IOLogger(logger)
}
