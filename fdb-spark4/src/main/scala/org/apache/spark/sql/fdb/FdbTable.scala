package org.apache.spark.sql.fdb

import com.goodcover.fdb.record.RecordDatabase.FdbMetadata
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.*
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters.*

case class FdbTable(
  session: SparkSession,
  sourceProvider: String,
  catalogConf: CaseInsensitiveStringMap,
  tableWithMeta: FdbTable.MetadataWith[ReadConf]
) extends Table
    with SupportsRead
    with SupportsWrite
    with Logging {

  private val storage               = StorageHelper(tableWithMeta.dbConfig)
  private lazy val structFromRecord = storage.metaDataToColumns(tableWithMeta)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new WriteBuilder {
    override def build(): Write = new Write {
      override def toBatch: BatchWrite = new FdbBatchWrite(schema(), tableWithMeta)
    }
  }

  override def name(): String = s"$sourceProvider/${tableWithMeta.config.tableKeySpacePath}"

  override def schema(): StructType = structFromRecord

  override def columns(): Array[Column] =
    CatalogV2Util.structTypeToV2Columns(schema())

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.MICRO_BATCH_READ,
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // Copy over for the catalog oriented read-specific options
    val newConfig = tableWithMeta.copy(config = tableWithMeta.config.combine(ReadConf.fromOptions(options)))
    new FdbScanBuilder(
      schema(),
      newConfig,
      options
    )
  }

}

object FdbTable {

  case class MetadataWith[T](config: T, dbConfig: SparkFdbConfig, recordMetaData: FdbMetadata)

}
