package org.apache.spark.sql.fdb

import com.apple.foundationdb.record.{ EndpointType, TupleRange }
import com.apple.foundationdb.tuple.{ ByteArrayUtil2, Tuple }

case class FdbSerializableRange(
  low: Array[Byte],
  high: Array[Byte],
  lowEndpoint: EndpointType,
  highEndpoint: EndpointType,
  continuation: KeyBytes
) {

  override def toString: String = s"FdbSerializableRange(" +
    s"low=${ByteArrayUtil2.loggable(low)}, " +
    s"high=${ByteArrayUtil2.loggable(high)}, " +
    s"lowEndpoint=$lowEndpoint, " +
    s"highEndpoint=$highEndpoint, " +
    s"continuation=$continuation)"

  def toRange: TupleRange = {
    val lowt  = if (low == null) null else Tuple.fromBytes(low)
    val hight = if (high == null) null else Tuple.fromBytes(high)

    new TupleRange(lowt, hight, lowEndpoint, highEndpoint)
  }
}
