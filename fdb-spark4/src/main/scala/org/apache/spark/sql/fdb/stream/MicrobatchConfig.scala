package org.apache.spark.sql.fdb.stream

import org.apache.spark.sql.fdb.ReadConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Serializable config */
case class MicrobatchConfig(
  private val microbatchProviderClass: Option[String],
  microbatchStreamName: Option[String],
  microbatchMaxBatchSize: Option[Int],
  microbatchSnapshot: Option[Boolean],
  readConf: ReadConf
) {

  @transient lazy val microbatchProvider: Option[FdbMicrobatchProvider[FdbIndexPartition]] =
    microbatchProviderClass map { lpCls =>
      val cls = Class.forName(lpCls)
      cls
        .getDeclaredConstructor(classOf[Map[String, String]])
        .newInstance(readConf.options)
        .asInstanceOf[FdbMicrobatchProvider[FdbIndexPartition]]
    }
}

object MicrobatchConfig {
  final val MICROBATCH_PROVIDER_CLASS = "microbatchProviderClass"
  final val MICROBATCH_STREAM_NAME    = "microbatchStreamName"
  final val MICROBATCH_MAX_BATCH_SIZE = "microbatchMaxBatchSize"
  final val MICROBATCH_SNAPSHOT       = "microbatchSnapshot"

  def fromOptions(options: CaseInsensitiveStringMap, tableConfig: ReadConf): MicrobatchConfig = {
    val microbatchProviderClass = Option(options.get(MICROBATCH_PROVIDER_CLASS))
    val microbatchStreamName    = Option(options.get(MICROBATCH_STREAM_NAME))
    val microbatchMaxBatchSize  = Try(options.get(MICROBATCH_MAX_BATCH_SIZE).toInt).toOption
    val microbatchSnapshot      = Option(options.get(MICROBATCH_SNAPSHOT)).flatMap(_.toBooleanOption)

    val combinedTableConfigOptions = tableConfig.copy(options = tableConfig.options ++ options.asScala)

    MicrobatchConfig(
      microbatchProviderClass = microbatchProviderClass,
      microbatchStreamName = microbatchStreamName,
      microbatchMaxBatchSize = microbatchMaxBatchSize,
      microbatchSnapshot = microbatchSnapshot,
      readConf = combinedTableConfigOptions
    )
  }

  def fromOptions(options: CaseInsensitiveStringMap): MicrobatchConfig = {
    val tableConfig = ReadConf.fromOptions(options)
    fromOptions(options, tableConfig)
  }
}
