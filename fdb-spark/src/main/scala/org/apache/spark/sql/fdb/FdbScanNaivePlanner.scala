package org.apache.spark.sql.fdb

import org.apache.spark.sql.connector.read.{ Batch, InputPartition, PartitionReaderFactory }
import org.apache.spark.sql.fdb.FdbScanBuilder.QueryBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Scan that uses the record planner.
 *
 * I have yet to figure out how to actually partition something with that.
 */
class FdbScanNaivePlanner(
  schema: StructType,
  queryBuilder: QueryBuilder,
  config: FdbTable.MetadataWith[ReadConf],
  options: CaseInsensitiveStringMap
) extends FdbScanBase(
      schema,
      config,
      options,
    ) {

  override def name: String = "FdbNaivePlannerScan"

  override def description(): String =
    s"""$name: ${config.config.recordType}.${config.config.tableKeySpacePath}, scheme: $schema
       | - Fdb Filters: pk = ${queryBuilder.keys.mkString("'", "', '", "'")}""".stripMargin

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  /** Just do one input partition for now. */
  override def planInputPartitions(): Array[InputPartition] =
    Array(FdbInputPartition("0", "", config.config, schema, queryBuilder, 0L, ()))

  override def createReaderFactory(): PartitionReaderFactory =
    new FdbScanPartitionReaderFactory(schema, queryBuilder, config)
}
