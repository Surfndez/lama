package co.ledger.lama.bitcoin.common.models.transactor

import co.ledger.lama.bitcoin.transactor.protobuf
import io.circe.{Decoder, Encoder}

abstract class CoinSelectionStrategy(val name: String) {
  def toProto: protobuf.CoinSelector
}

object CoinSelectionStrategy {

  case object DepthFirst extends CoinSelectionStrategy("depth_first") {
    def toProto: protobuf.CoinSelector = protobuf.CoinSelector.DEPTH_FIRST
  }

  case object OptimizeSize extends CoinSelectionStrategy("optimize_size") {
    def toProto: protobuf.CoinSelector = protobuf.CoinSelector.OPTIMIZE_SIZE
  }

  val all: Map[String, CoinSelectionStrategy] = Map(
    DepthFirst.name   -> DepthFirst,
    OptimizeSize.name -> OptimizeSize
  )

  def fromKey(key: String): Option[CoinSelectionStrategy] = all.get(key)

  implicit val encoder: Encoder[CoinSelectionStrategy] = Encoder.encodeString.contramap(_.name)

  implicit val decoder: Decoder[CoinSelectionStrategy] =
    Decoder.decodeString.emap(fromKey(_).toRight("unable to decode coin selection strategy"))

  def fromProto(proto: protobuf.CoinSelector): CoinSelectionStrategy =
    proto match {
      case protobuf.CoinSelector.OPTIMIZE_SIZE => OptimizeSize
      case _                                   => DepthFirst
    }
}
