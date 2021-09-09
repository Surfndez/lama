package co.ledger.lama.common

import co.ledger.lama.common.utils.HexUtils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HexUtilsSpec extends AnyFunSuite with Matchers {
  test("hex to bytes") {
    val rawTx =
      "010000000168603796BEA6C4FDF93FBF62C00B17D5C6CE698315" +
        "AFFEB47FD234B5051647180100000000FFFFFFFF02A086010000000000" +
        "1600140163371FED6FF4D2AC62C6F93B3A36E2450786B773020C000000" +
        "00001600148AFB601974D12AD8D4F2FBC47E8D13448EB76A3F00000000"

    val bytes = HexUtils.valueOf(rawTx)

    assert(rawTx == HexUtils.valueOf(bytes))
  }
}
