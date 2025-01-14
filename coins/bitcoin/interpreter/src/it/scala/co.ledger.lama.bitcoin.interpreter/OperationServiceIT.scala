package co.ledger.lama.bitcoin.interpreter

import java.time.Instant
import java.util.UUID

import cats.data.NonEmptyList
import co.ledger.lama.common.logging.LamaLogContext
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.interpreter.services.{FlaggingService, OperationService}
import co.ledger.lama.common.models.{PaginationToken, Sort}
import co.ledger.lama.common.utils.IOAssertion
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class OperationServiceIT extends AnyFlatSpecLike with Matchers with TestResources {

  val accountId: UUID = UUID.fromString("b723c553-3a9a-4130-8883-ee2f6c2f9202")

  implicit val lc: LamaLogContext = LamaLogContext().withAccountId(accountId)

  private val outputAddress1 =
    AccountAddress("1DtwACvd338XtHBFYJRVKRLxviD7YtYADa", ChangeType.External, NonEmptyList.of(1, 0))
  private val outputAddress2 =
    AccountAddress("1LK8UbiRwUzC8KFEbMKvgbvriM9zLMce3C", ChangeType.Internal, NonEmptyList.of(0, 0))
  private val inputAddress =
    AccountAddress("1LD1pARePgXXyZA1J3EyvRtB82vxENs5wQ", ChangeType.External, NonEmptyList.of(1, 1))

  val block1: BlockView = BlockView(
    "00000000000000000008c76a28e115319fb747eb29a7e0794526d0fe47608379",
    570153,
    Instant.parse("2019-04-04T10:03:22Z")
  )

  val block2: BlockView = BlockView(
    "00000000000000000003d16980a4ec530adf4bcefc74ca149a2b1788444e9c3a",
    650909,
    Instant.parse("2020-10-02T11:17:48Z")
  )

  val outputs = List(
    OutputView(0, 50000, outputAddress1.accountAddress, "script", None, None),
    OutputView(1, 9434, outputAddress2.accountAddress, "script", None, None)
  )

  val inputs = List(
    InputView(
      "0f38e5f1b12078495a9e80c6e0d77af3d674cfe6096bb6e7909993a53b6e8386",
      0,
      0,
      80000,
      inputAddress.accountAddress,
      "script",
      List(),
      4294967295L,
      None
    )
  )

  val insertTx1: TransactionView =
    TransactionView(
      "txId1",
      "a8a935c6bc2bd8b3a7c20f107a9eb5f10a315ce27de9d72f3f4e27ac9ec1eb1f",
      Instant.parse("2019-04-04T10:03:22Z"),
      0,
      20566,
      inputs,
      outputs,
      Some(block1),
      1
    )

  val insertTx2: TransactionView =
    TransactionView(
      "txId2",
      "b0c0dc176eaf463a5cecf15f1f55af99a41edfd6e01685068c0db3cc779861c8",
      Instant.parse("2019-04-04T10:03:22Z"),
      0,
      30566,
      inputs,
      outputs,
      Some(block2),
      1
    )

  "operation saved in db" should "be fetched" in IOAssertion {
    setup() *>
      appResources.use { db =>
        val operationService = new OperationService(db)
        val flaggingService  = new FlaggingService(db)

        for {
          _ <- ITUtils.saveTx(db, insertTx1, accountId)
          _ <- flaggingService.flagInputsAndOutputs(accountId, List(inputAddress, outputAddress2))
          _ <- ITUtils.compute(operationService, accountId)

          res <- operationService.getOperations(
            accountId,
            limit = 20,
            sort = Sort.Ascending,
            None
          )
          GetOperationsResult(ops, total, cursor) = res
        } yield {
          ops should have size 1
          total shouldBe 1
          cursor shouldBe None

          val op = ops.head
          val tx = op.transaction

          op.accountId shouldBe accountId
          op.hash shouldBe insertTx1.hash
          op.operationType shouldBe OperationType.Send

          tx.fees shouldBe insertTx1.fees

          tx.inputs.find(_.belongs).get.address shouldBe inputAddress.accountAddress
          tx.outputs.find(_.belongs).get.address shouldBe outputAddress2.accountAddress
          tx.outputs.find(_.belongs).get.changeType shouldBe Some(ChangeType.Internal)
        }
      }
  }

  it should "be fetched by uid" in IOAssertion {

    setup() *>
      appResources.use { db =>
        val operationService = new OperationService(db)
        val flaggingService  = new FlaggingService(db)

        for {
          _ <- ITUtils.saveTx(db, insertTx1, accountId)
          _ <- flaggingService.flagInputsAndOutputs(accountId, List(inputAddress, outputAddress2))
          _ <- ITUtils.compute(operationService, accountId)
          foundOperation <- operationService.getOperation(
            Operation.AccountId(accountId),
            Operation.uid(
              Operation.AccountId(accountId),
              Operation.TxId(
                insertTx1.hash // because of compute which  put tx.hash in operation.hash instead of txid
              ),
              OperationType.Send,
              insertTx1.block.map(_.height)
            )
          )

        } yield {

          foundOperation should not be None
          val Some(op) = foundOperation

          val tx = op.transaction

          op.accountId shouldBe accountId
          op.hash shouldBe insertTx1.hash
          op.operationType shouldBe OperationType.Send

          tx.fees shouldBe insertTx1.fees

          tx.inputs.find(_.belongs).get.address shouldBe inputAddress.accountAddress
          tx.outputs.find(_.belongs).get.address shouldBe outputAddress2.accountAddress
          tx.outputs.find(_.belongs).get.changeType shouldBe Some(ChangeType.Internal)
        }
      }

  }

  it should "fetched only ops from a cursor" in IOAssertion {
    setup() *>
      appResources.use { db =>
        val operationService = new OperationService(db)
        val flaggingService  = new FlaggingService(db)

        for {
          _ <- ITUtils.saveTx(db, insertTx1, accountId)
          _ <- ITUtils.saveTx(db, insertTx2, accountId)
          _ <- flaggingService.flagInputsAndOutputs(accountId, List(inputAddress, outputAddress2))
          _ <- ITUtils.compute(operationService, accountId)

          resFirstPage <- operationService.getOperations(
            accountId,
            limit = 1,
            sort = Sort.Ascending,
            None
          )

          firstPageNextCursor = resFirstPage.cursor
            .flatMap(_.next)
            .flatMap(PaginationToken.fromBase64[OperationPaginationState](_))

          resLastPage <- operationService.getOperations(
            accountId,
            limit = 1,
            sort = Sort.Ascending,
            firstPageNextCursor
          )

          GetOperationsResult(ops, total, lastPageCursor) = resLastPage
        } yield {

          firstPageNextCursor shouldBe Some(
            PaginationToken(
              OperationPaginationState(
                uid = resFirstPage.operations.last.uid,
                blockHeight = block1.height
              ),
              isNext = true
            )
          )

          ops should have size 1
          total shouldBe 2
          lastPageCursor.flatMap(_.next) shouldBe None

          val op = ops.head
          val tx = op.transaction

          op.accountId shouldBe accountId
          op.hash shouldBe insertTx2.hash
          op.operationType shouldBe OperationType.Send

          tx.fees shouldBe insertTx2.fees

          tx.inputs.find(_.belongs).get.address shouldBe inputAddress.accountAddress
          tx.outputs.find(_.belongs).get.address shouldBe outputAddress2.accountAddress
          tx.outputs.find(_.belongs).get.changeType shouldBe Some(ChangeType.Internal)
        }
      }
  }

  it should "have made utxos" in IOAssertion {
    setup() *>
      appResources.use { db =>
        val operationService = new OperationService(db)
        val flaggingService  = new FlaggingService(db)

        for {
          _   <- ITUtils.saveTx(db, insertTx1, accountId)
          _   <- flaggingService.flagInputsAndOutputs(accountId, List(inputAddress, outputAddress1))
          _   <- ITUtils.compute(operationService, accountId)
          res <- operationService.getUtxos(accountId, Sort.Ascending, 20, 0)
          GetUtxosResult(utxos, total, trunc) = res
        } yield {
          utxos should have size 1
          total shouldBe 1
          trunc shouldBe false

          val utxo = utxos.head

          utxo.address shouldBe outputAddress1.accountAddress
          utxo.changeType shouldBe Some(outputAddress1.changeType)
        }
      }
  }

  "unconfirmed Transactions" should "have made utxos" in IOAssertion {
    setup() *>
      appResources.use { db =>
        val operationService = new OperationService(db)

        val unconfirmedTransaction1 = TransactionView(
          "txId1",
          "txHash1",
          Instant.now,
          0L,
          0,
          List(
            InputView(
              "someOtherTransaction",
              0,
              0,
              1000,
              "notMyAddress",
              "script",
              Nil,
              Int.MaxValue,
              None
            )
          ),
          List(
            OutputView( //create UTXO
              0,
              1000,
              "myAddress",
              "script",
              Some(ChangeType.External),
              Some(NonEmptyList(0, List(0)))
            )
          ),
          None,
          0
        )

        val unconfirmedTransaction2 = TransactionView(
          "txId2",
          "txHash2",
          Instant.now,
          0L,
          0,
          List(
            InputView( //using previously made UTXO
              "txHash1",
              0,
              0,
              1000,
              "myAddress",
              "script",
              Nil,
              Int.MaxValue,
              Some(NonEmptyList(0, List(0)))
            )
          ),
          List( //creating 2 new UTXOs
            OutputView(
              0,
              250,
              "myAddress2",
              "script",
              Some(ChangeType.Internal),
              Some(NonEmptyList(1, List(0)))
            ),
            OutputView(
              1,
              250,
              "myAddress3",
              "script",
              Some(ChangeType.External),
              Some(NonEmptyList(0, List(2)))
            ),
            OutputView(
              0,
              800,
              "notMyAddress",
              "script",
              None,
              None
            )
          ),
          None,
          0
        )

        for {
          _     <- ITUtils.saveTx(db, unconfirmedTransaction1, accountId)
          _     <- ITUtils.saveTx(db, unconfirmedTransaction2, accountId)
          utxos <- operationService.getUnconfirmedUtxos(accountId)
        } yield {
          utxos should have size 2
          utxos.map(_.value).sum should be(500)
        }
      }
  }

}
