package org.apache.spark.sql.fdb

import com.apple.foundationdb.record.query.expressions.QueryComponent

sealed trait InputPartitionScheme extends Serializable

trait QueryRestrictionScheme extends InputPartitionScheme {
  def queryAnd: QueryComponent
}

trait PrimaryRestrictionScheme extends InputPartitionScheme {
  def range: FdbSerializableRange
}

trait PartitionScheme extends Product with Serializable {
  def scheme: PartitionScheme.SchemeType

  def partitions(metaData: FdbTable.MetadataWith[ReadConf]): Array[InputPartitionScheme]
}

object PartitionScheme {
  sealed abstract class SchemeType(val pushDown: Boolean) extends Serializable with Product
  case object QueryRestrictionEnum                        extends SchemeType(true)
  case object PrimaryRestrictionEnum                      extends SchemeType(false)
}
