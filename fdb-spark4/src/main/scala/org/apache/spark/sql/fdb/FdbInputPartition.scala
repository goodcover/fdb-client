package org.apache.spark.sql.fdb

import com.apple.foundationdb.tuple.ByteArrayUtil
import com.apple.foundationdb.Range as FDBRange
import com.goodcover.fdb.Helpers
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.fdb.FdbScanBuilder.QueryBuilder
import org.apache.spark.sql.types.StructType

case class FdbInputPartition(
  partitionId: String,
  key: String,
  config: ReadConf,
  schema: StructType,
  // Potentially nuke QB
  filters: QueryBuilder,
  estimatedSize: Long,
  // Kludgey
  additionalPartitionInformation: Any
) extends InputPartition {

  override def preferredLocations(): Array[String] = Array.empty

  def begin: Array[Byte] = Array()

  def end: Array[Byte] = Array()

  override def toString: String =
    s"FdbInputPartition(partitionId=$partitionId,key=$key,begin=${ByteArrayUtil
        .printable(begin)}, end=${ByteArrayUtil.printable(end)}, estimate=${Helpers.humanReadableSize(estimatedSize, false)})"

}

/** A serializable version of the [[FDBRange]] */
case class LocalRange(begin: Array[Byte], end: Array[Byte]) {
  def toRange: FDBRange = new FDBRange(begin, end)

  override def toString: String =
    s"LocalRange(begin=${ByteArrayUtil.printable(begin)}, end=${ByteArrayUtil.printable(end)})"
}

object LocalRange {
  def apply(range: FDBRange): LocalRange = LocalRange(range.begin, range.end)
}
