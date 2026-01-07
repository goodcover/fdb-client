package org.apache.spark.sql.fdb.stream

import com.apple.foundationdb.tuple.ByteArrayUtil2
import org.apache.spark.sql.connector.read.streaming.Offset
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import org.apache.spark.sql.fdb.KeyBytes

import java.util

case class FdbStreamOffsets(batchId: Long, name: String, seq: Seq[(Long, FdbStreamOffsets.SingleContinuation)]) extends Offset {

  override def json(): String =
    writeToString(this)
}

object FdbStreamOffsets {

  implicit val FdbStreamOffsetsCodec: JsonValueCodec[FdbStreamOffsets] = JsonCodecMaker.make

  def fromJson(json: String): FdbStreamOffsets =
    readFromString[FdbStreamOffsets](json)

  case class SingleContinuation(shardId: Long, low: KeyBytes, continuation: KeyBytes) extends Comparable[SingleContinuation] {
    override def compareTo(o: SingleContinuation): Int = {
      val first = shardId.compare(o.shardId)
      if (first == 0) {
        val second = low.compareTo(o.low)
        if (second == 0) {
          continuation.compareTo(o.continuation)
        } else second
      } else first
    }

    override def equals(obj: Any): Boolean =
      obj match {
        case e: SingleContinuation => compareTo(e) == 0
        case _                     => obj == this
      }
  }

  object SingleContinuation {
    implicit val SingleContinuationCodec: JsonValueCodec[SingleContinuation] = JsonCodecMaker.make
  }

}
