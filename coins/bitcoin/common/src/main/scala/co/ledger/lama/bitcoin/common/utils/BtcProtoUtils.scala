package co.ledger.lama.bitcoin.common.utils

import co.ledger.lama.bitcoin.interpreter.protobuf
import co.ledger.lama.common.models

object BtcProtoUtils {

  implicit class SortProtoUtils(sort: models.Sort) {
    def toProto: protobuf.SortingOrder = {
      sort match {
        case models.Sort.Ascending => protobuf.SortingOrder.ASC
        case _                     => protobuf.SortingOrder.DESC
      }
    }
  }

  object Sort {
    def fromProto(proto: protobuf.SortingOrder): models.Sort =
      proto match {
        case protobuf.SortingOrder.ASC => models.Sort.Ascending
        case _                         => models.Sort.Descending
      }
  }

  implicit class AccountBtcProtoUtils(account: models.Account) {
    def toBtcProto: protobuf.Account = {
      protobuf.Account(
        account.identifier,
        account.coinFamily.name,
        account.coin.name,
        account.group.name
      )
    }
  }

  object BtcAccount {
    def fromBtcProto(proto: protobuf.Account): models.Account =
      models.Account(
        proto.identifier,
        models.CoinFamily.fromKey(proto.coinFamily).get,
        models.Coin.fromKey(proto.coin).get,
        models.AccountGroup(proto.group)
      )
  }
}
