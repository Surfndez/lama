package co.ledger.lama.common.utils

object HexUtils {
  private val SLIDING_SIZE = 2
  private val SLIDING_STEP = 2
  private val PARSE_RADIX  = 16

  def valueOf(array: Array[Byte]): String = array.map("%02X" format _).mkString

  def valueOf(string: String): Array[Byte] = string.toSeq
    .sliding(SLIDING_SIZE, SLIDING_STEP)
    .toArray
    .map(s => Integer.parseInt(s.unwrap, PARSE_RADIX).toByte)
}
