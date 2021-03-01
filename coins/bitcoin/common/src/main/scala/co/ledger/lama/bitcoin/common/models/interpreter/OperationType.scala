package co.ledger.lama.bitcoin.common.models.interpreter

import co.ledger.lama.bitcoin.interpreter.protobuf
import io.circe.{Decoder, Encoder}

sealed trait OperationType {
  val name: String
  def toProto: protobuf.OperationType
}

object OperationType {

  case object Send extends OperationType {
    val name = "send"
    def toProto: protobuf.OperationType = {
      protobuf.OperationType.SEND
    }
  }

  case object Receive extends OperationType {
    val name = "receive"
    def toProto: protobuf.OperationType = {
      protobuf.OperationType.RECEIVE
    }
  }

  implicit val encoder: Encoder[OperationType] = Encoder.encodeString.contramap(_.name)
  implicit val decoder: Decoder[OperationType] =
    Decoder.decodeString.emap(fromKey(_).toRight("Could not decode as operation type"))

  val all: Map[String, OperationType] = Map(Send.name -> Send, Receive.name -> Receive)

  def fromKey(key: String): Option[OperationType] = all.get(key)

  def fromProto(proto: protobuf.OperationType): OperationType = {
    proto match {
      case protobuf.OperationType.SEND => Send
      case _                           => Receive

    }
  }
}
