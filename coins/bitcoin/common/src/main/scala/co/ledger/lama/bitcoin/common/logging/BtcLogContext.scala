package co.ledger.lama.bitcoin.common.logging

import java.util.UUID

import co.ledger.lama.common.logging.LogContext
import co.ledger.lama.common.models.{Account, AccountGroup, Coin, CoinFamily}

case class BtcLogContext(
    accountId: Option[UUID] = None,
    keychainId: Option[String] = None,
    coinFamily: Option[CoinFamily] = None,
    coin: Option[Coin] = None,
    group: Option[AccountGroup] = None,
    syncId: Option[UUID] = None
) extends LogContext {

  def withAccount(account: Account): BtcLogContext =
    this.copy(
      accountId = Some(account.id),
      keychainId = Some(account.identifier),
      coinFamily = Some(account.coinFamily),
      coin = Some(account.coin),
      group = Some(account.group)
    )

  def withAccountId(accountId: UUID): BtcLogContext =
    this.copy(accountId = Some(accountId))

  def withKeychainId(keychainId: UUID): BtcLogContext =
    this.copy(keychainId = Some(keychainId.toString))

  def withCoinFamily(coinFamily: CoinFamily): BtcLogContext =
    this.copy(coinFamily = Some(coinFamily))

  def withCoin(coin: Coin): BtcLogContext =
    this.copy(coin = Some(coin))

  def withGroup(group: AccountGroup): BtcLogContext =
    this.copy(group = Some(group))

  def withSyncId(syncId: UUID): BtcLogContext =
    this.copy(syncId = Some(syncId))

  override def asMap(): Map[String, String] = List(
    accountId.map(account => ("id", account.toString)),
    keychainId.map(id => ("keychainId", id)),
    coinFamily.map(cf => ("coin_family", cf.name)),
    coin.map(c => ("coin", c.name)),
    group.map(g => ("group", g.name)),
    syncId.map(id => ("syncId", id.toString))
  ).flatten.toMap

}
