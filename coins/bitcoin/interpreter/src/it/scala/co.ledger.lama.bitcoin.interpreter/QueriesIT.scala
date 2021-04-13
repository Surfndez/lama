package co.ledger.lama.bitcoin.interpreter

import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.interpreter.models.OperationToSave
import co.ledger.lama.bitcoin.interpreter.services.OperationQueries.Op
import co.ledger.lama.common.models.TxHash
import co.ledger.lama.common.utils.IOAssertion
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID
import scala.util.Random

class QueriesIT extends AnyFlatSpecLike with Matchers with TestResources {

  val accountId: UUID = UUID.fromString("b723c553-3a9a-4130-8883-ee2f6c2f9201")

  def randomAlphanumeric(length: Int): String =
    Random.alphanumeric.take(length).mkString

  def generateBlock(): BlockView = {
    val blockHash   = randomAlphanumeric(10)
    val blockHeight = Random.nextInt(5)
    BlockView(
      blockHash,
      blockHeight,
      Instant.parse("2019-04-04T10:03:22Z")
    )
  }

  def generateTransaction(): TransactionView = {
    val txId = randomAlphanumeric(10)

    val outputs = List(
      OutputView(0, 50000, randomAlphanumeric(10), "script", None, None),
      OutputView(1, 9434, randomAlphanumeric(10), "script", None, None)
    )
    val inputs = List(
      InputView(
        randomAlphanumeric(10),
        0,
        0,
        80000,
        "1LD1pARePgXXyZA1J3EyvRtB82vxENs5wQ",
        "script",
        List(),
        4294967295L,
        None
      )
    )

    val block = generateBlock()

    TransactionView(
      txId,
      txId,
      Instant.parse("2019-04-04T10:03:22Z"),
      0,
      20566,
      inputs,
      outputs,
      Some(block),
      1
    )
  }

  val transactionToInsert: TransactionView = generateTransaction()

  "transaction saved in db" should "not be returned and populated by fetch before Interpreter.compute" in IOAssertion {
    setup() *>
      appResources.use { db =>
        for {
          _ <- QueryUtils.saveTx(db, transactionToInsert, accountId)
          _ <- QueryUtils.saveTx(db, transactionToInsert, accountId) // check upsert
          inputsWithOutputs <- QueryUtils.fetchInputAndOutputs(
            db,
            accountId,
            TxHash(transactionToInsert.hash)
          )

          account = Operation.AccountId(accountId)

          opWithTx <- QueryUtils.fetchOpAndTx(
            db,
            account,
            Operation.uid(
              account,
              Operation.TxId(transactionToInsert.hash),
              opToSave.operationType,
              opToSave.blockHeight
            )
          )
        } yield {

          opWithTx shouldBe None
          inputsWithOutputs should not be empty

          val Some((inputs, outputs)) = inputsWithOutputs

          inputs should have size 1
          inputs.head.value shouldBe 80000

          outputs should have size 2
          outputs.filter(_.outputIndex == 0).head.value shouldBe 50000
        }
      }
  }

  val opToSave: OperationToSave = OperationToSave(
    Operation.uid(
      Operation.AccountId(accountId),
      Operation.TxId(transactionToInsert.id),
      OperationType.Send,
      transactionToInsert.block.map(_.height)
    ),
    accountId,
    transactionToInsert.hash,
    OperationType.Send,
    transactionToInsert.inputs.collect { case i: InputView =>
      i.value
    }.sum,
    transactionToInsert.fees,
    transactionToInsert.block.get.time,
    transactionToInsert.block.map(_.hash),
    transactionToInsert.block.map(_.height)
  )

  "operation saved in db" should "be fetched" in IOAssertion {
    setup() *>
      appResources.use { db =>
        for {
          _   <- QueryUtils.saveTx(db, transactionToInsert, accountId)
          _   <- QueryUtils.saveOp(db, opToSave)
          ops <- QueryUtils.fetchOps(db, accountId)
        } yield {
          ops.map(_.op) should contain only
            Op(
              opToSave.uid,
              opToSave.accountId,
              TxHash(opToSave.hash),
              opToSave.operationType,
              opToSave.value,
              opToSave.fees,
              opToSave.time,
              opToSave.blockHeight
            )
        }
      }
  }

}
