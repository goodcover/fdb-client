package org.apache.spark.sql.fdb

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.MapHasAsScala

class FdbProvider extends SimpleTableProvider with DataSourceRegister {
  private lazy val sparkSession = SparkSession.active

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val cfg         = SparkFdbConfig.fromMap(options.asCaseSensitiveMap().asScala.toMap)
    val tableConfig = ReadConf.fromOptions(options)

    val helper = StorageHelper(cfg)

    val metaData = helper.metaData(tableConfig) match {
      case Some(value) => value
      case None        =>
        throw new IllegalStateException(
          s"Missing metadata at the location ${tableConfig.metaKeySpacePath}, you'll need to manually initialize it outside of spark."
        )
    }

    FdbTable(sparkSession, shortName(), options, FdbTable.MetadataWith(tableConfig, cfg, metaData))
  }

  override def shortName(): String = FdbProvider.FDB_PLAIN
}

object FdbProvider {
  val FDB_PLAIN = "fdb-plain"
}
