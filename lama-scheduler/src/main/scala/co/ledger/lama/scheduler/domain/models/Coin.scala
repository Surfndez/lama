package co.ledger.lama.scheduler.domain.models

import cats.effect.IO
import io.circe.{Decoder, Encoder}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

sealed abstract class Coin(val name: String, val coinFamily: CoinFamily) {
  override def toString: String = name
}

sealed abstract class BitcoinLikeCoin(name: String) extends Coin(name, CoinFamily.Bitcoin)

object BitcoinLikeCoin {
  def fromKeyIO(key: String): IO[BitcoinLikeCoin] = IO.fromOption(Coin.fromKey(key) collect {
    case c: BitcoinLikeCoin => c
  })(
    new IllegalArgumentException(
      s"Coin ${key} is not BitcoinLike, or unknown coin"
    )
  )
}

object Coin {
  case object Btc        extends BitcoinLikeCoin("btc")
  case object BtcTestnet extends BitcoinLikeCoin("btc_testnet")
  case object BtcRegtest extends BitcoinLikeCoin("btc_regtest")
  case object Ltc        extends BitcoinLikeCoin("ltc")

  val all: Map[String, Coin] = Map(
    Btc.name        -> Btc,
    BtcTestnet.name -> BtcTestnet,
    BtcRegtest.name -> BtcRegtest,
    Ltc.name        -> Ltc
  )

  def fromKey(key: String): Option[Coin] = all.get(key)

  def fromKeyIO(key: String): IO[Coin] = IO.fromOption(fromKey(key))(
    new IllegalArgumentException(
      s"Unknown coin type $key) in CreateTransactionRequest"
    )
  )

  implicit val encoder: Encoder[Coin] =
    Encoder.encodeString.contramap(_.name)

  implicit val decoder: Decoder[Coin] =
    Decoder.decodeString.emap(fromKey(_).toRight("Could not decode as coin"))

  implicit val configReader: ConfigReader[Coin] =
    ConfigReader.fromString(str => fromKey(str).toRight(CannotConvert(str, "Coin", "unknown")))
}
