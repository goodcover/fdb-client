package org.apache.spark.sql.fdb

import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{ Batch, Scan }
import org.apache.spark.sql.fdb.stream.{ FdbMicrobatchStream, MicrobatchConfig }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.*

abstract class FdbScanBase(
  schema: StructType,
  config: FdbTable.MetadataWith[ReadConf],
  options: CaseInsensitiveStringMap
) extends Scan
    with Batch {

  def name: String

  override def toBatch: Batch = this

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    val combinedMap = config.config.options ++ options.asScala
    val mbConfig    = MicrobatchConfig.fromOptions(new CaseInsensitiveStringMap(combinedMap.asJava), config.config)
    val mbp         = mbConfig.microbatchProvider.getOrElse(
      throw new IllegalStateException(
        "Missing configuration for microbatchProviderClass," +
          " you need to specify one to use microbatch streaming."
      )
    )
    new FdbMicrobatchStream(schema, conf = config.copy(mbConfig), checkpointLocation, provider = mbp)
  }

}
