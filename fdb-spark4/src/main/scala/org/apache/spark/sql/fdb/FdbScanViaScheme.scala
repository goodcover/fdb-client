package org.apache.spark.sql.fdb

import org.apache.spark.sql.connector.read.{ Batch, InputPartition, PartitionReaderFactory }
import org.apache.spark.sql.fdb.FdbScanBuilder.QueryBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FdbScanViaScheme(
  schema: StructType,
  queryBuilder: QueryBuilder,
  config: FdbTable.MetadataWith[ReadConf],
  options: CaseInsensitiveStringMap
) extends FdbScanBase(
      schema,
      config,
      options,
    ) {

  override def name: String = "FdbScanViaScheme"

  override def description(): String =
    s"""$name: ${config.config.recordType}.${config.config.tableKeySpacePath}, scheme: $schema
       | - Fdb Filters: pk = ${queryBuilder.keys.mkString("'", "', '", "'")}""".stripMargin

  override def readSchema(): StructType = schema

  /** Just do one input partition for now. */
  override def planInputPartitions(): Array[InputPartition] =
    config.config.partitionScheme.get.partitions(config).zipWithIndex.map { case (ips, i) =>
      FdbInputPartition(i.toString, i.toString, config.config, schema, queryBuilder, 0L, ips)
    }

  override def createReaderFactory(): PartitionReaderFactory =
    new FdbScanPartitionReaderFactory(schema, queryBuilder, config)

}
