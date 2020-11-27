package co.ledger.lama.bitcoin.transactor.services
import cats.effect.IO
import co.ledger.lama.bitcoin.transactor.models.bitcoinLib

class BitcoinLibGrpcClientServiceMock extends BitcoinLibGrpcService {

  def createTransaction(
      transaction: bitcoinLib.CreateTransactionRequest
  ): IO[bitcoinLib.CreateTransactionResponse] = {
    IO(
      bitcoinLib.CreateTransactionResponse(
        "hex",
        "hash",
        "witnessHash"
      )
    )
  }

}
