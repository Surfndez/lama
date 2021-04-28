package co.ledger.lama.common.logging

import java.util.UUID

import co.ledger.lama.common.models.{Account, AccountGroup, Coin, CoinFamily}

case class LamaLogContext(
    accountId: Option[UUID] = None,
    identifier: Option[String] = None,
    coinFamily: Option[CoinFamily] = None,
    coin: Option[Coin] = None,
    group: Option[AccountGroup] = None,
    followUpId: Option[UUID] = None,
    customFields: List[Option[(String, String)]] = List()
) extends LogContext {

  def withAccount(account: Account): LamaLogContext =
    copy(
      accountId = Some(account.id),
      identifier = Some(account.identifier),
      coinFamily = Some(account.coinFamily),
      coin = Some(account.coin),
      group = Some(account.group)
    )

  def withAccountId(accountId: UUID): LamaLogContext =
    copy(accountId = Some(accountId))

  def withIdentifier(identifier: String): LamaLogContext =
    copy(identifier = Some(identifier))

  def withCoinFamily(coinFamily: CoinFamily): LamaLogContext =
    copy(coinFamily = Some(coinFamily))

  def withCoin(coin: Coin): LamaLogContext =
    copy(coin = Some(coin))

  def withGroup(group: AccountGroup): LamaLogContext =
    copy(group = Some(group))

  def withFollowUpId(followUpId: UUID): LamaLogContext =
    copy(followUpId = Some(followUpId))

  def withCustomField(key: String, value: String): LamaLogContext =
    copy(customFields = Some((key, value)) :: customFields)

  override def asMap(): Map[String, String] = (List(
    accountId.map(account => ("id", account.toString)),
    identifier.map(id => ("identifier", id)),
    coinFamily.map(cf => ("coin_family", cf.name)),
    coin.map(c => ("coin", c.name)),
    group.map(g => ("group", g.name)),
    followUpId.map(id => ("followUpId", id.toString))
  ) ::: customFields).flatten.toMap

}
