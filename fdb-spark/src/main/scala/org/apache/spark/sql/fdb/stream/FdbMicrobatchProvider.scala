package org.apache.spark.sql.fdb.stream

import com.apple.foundationdb.record.TupleRange
import org.apache.spark.sql.connector.read.InputPartition

/**
 * This is a hard thing to wrap into a serializable thing so working off a class
 * representation for now
 */
abstract class FdbMicrobatchProvider[T <: FdbPartition] {

  def partitions: Array[T]
}

sealed trait FdbPartition extends InputPartition

trait FdbIndexPartition extends FdbPartition {

  def shardId: Long

  def indexTuple: TupleRange

  def indexName: String

}
