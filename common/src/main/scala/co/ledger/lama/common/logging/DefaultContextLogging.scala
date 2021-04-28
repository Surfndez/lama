package co.ledger.lama.common.logging

trait DefaultContextLogging extends ContextLogging {
  implicit val lc: LamaLogContext = LamaLogContext()
}
