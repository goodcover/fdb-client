package org.apache.spark.sql.fdb.stream

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.fdb.FdbSerializableRange

case class FdbMicrobatchInputPartition(indexName: String, batchId: Long, shardId: Long, range: FdbSerializableRange)
    extends InputPartition() {

  override def toString: String =
    s"FdbMicrobatchInputPartition(" +
      s"indexName=$indexName, " +
      s"batchId=$batchId, " +
      s"shardId=$shardId, " +
      s"range=$range)"
}
