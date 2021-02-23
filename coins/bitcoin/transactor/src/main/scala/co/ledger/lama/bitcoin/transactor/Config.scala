package co.ledger.lama.bitcoin.transactor

import co.ledger.lama.bitcoin.common.config.ExplorerConfig
import co.ledger.lama.common.utils.{GrpcClientConfig, GrpcServerConfig}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class Config(
    grpcServer: GrpcServerConfig,
    explorer: ExplorerConfig,
    interpreter: GrpcClientConfig,
    keychain: GrpcClientConfig,
    bitcoinLib: GrpcClientConfig,
    transactor: TransactorConfig
)

object Config {
  implicit val configReader: ConfigReader[Config] = deriveReader[Config]
}

case class TransactorConfig(
    maxUtxos: Int
)

object TransactorConfig {
  implicit val configReader: ConfigReader[TransactorConfig] = deriveReader[TransactorConfig]
}
