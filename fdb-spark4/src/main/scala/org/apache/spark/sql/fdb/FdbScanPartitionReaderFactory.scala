package org.apache.spark.sql.fdb

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{ InputPartition, PartitionReader, PartitionReaderFactory }
import org.apache.spark.sql.fdb.FdbScanBuilder.QueryBuilder
import org.apache.spark.sql.types.StructType

class FdbScanPartitionReaderFactory(schema: StructType, queryBuilder: QueryBuilder, config: FdbTable.MetadataWith[ReadConf])
    extends PartitionReaderFactory {

  private val localConfig = config.dbConfig
  private val tableConfig = config.config

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case fdbInput @ FdbInputPartition(_, _, _, _, _, _, ips: PrimaryRestrictionScheme) =>
        if (queryBuilder.selectedFields.isEmpty && queryBuilder.keys.isEmpty && queryBuilder.hasRecordTypeCountIndex)
          new FdbScanPrimaryKeyPartitionReaderCount(fdbInput, localConfig, ips, tableConfig)
        else
          new FdbScanPrimaryKeyPartitionReader(fdbInput, localConfig, ips, tableConfig)

      case fdbInput @ FdbInputPartition(_, _, _, _, _, _, ips: QueryRestrictionScheme) =>
        new FdbScanNaivePartitionReader(fdbInput, localConfig, Some(ips.queryAnd), tableConfig)

      case fdbInput: FdbInputPartition =>
        if (queryBuilder.selectedFields.isEmpty && queryBuilder.keys.isEmpty && queryBuilder.hasRecordTypeCountIndex)
          new FdbScanCountPartitionReader(localConfig, tableConfig)
        else
          new FdbScanNaivePartitionReader(fdbInput, localConfig, None, tableConfig)

      case _ =>
        throw new Exception("Input partition not recognized")
    }

}
