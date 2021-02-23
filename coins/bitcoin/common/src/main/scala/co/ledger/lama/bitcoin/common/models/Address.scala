package co.ledger.lama.bitcoin.common.models

case class Address(value: String) extends AnyVal
case class InvalidAddress(address: Address, reason: String)
