package org.apache.spark.sql.fdb

import com.goodcover.fdb.{ FoundationDbConfig, LayerProvider }
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.*

/**
 * Connection and setup oriented options, including base layering
 */
case class SparkFdbConfig(
  dbConfig: FoundationDbConfig,
  private val layerProviderClass: Option[String],
  releaseDelayMs: Int,
  options: Map[String, String]
) {

  @transient lazy val layerProvider: LayerProvider =
    layerProviderClass match {
      case Some(lpCls) =>
        val cls = Class.forName(lpCls)
        cls.getDeclaredConstructor().newInstance().asInstanceOf[LayerProvider]
      case None        =>
        new LayerProvider.DefaultLayerProvider

    }
}

object SparkFdbConfig {
  final val LAYER_PROVIDER_CLASS = "layerProviderClass"
  final val RELEASE_DELAY_MS     = "releaseDelayMs"

  def fromMap(options: Map[String, String], dbConfig: FoundationDbConfig): SparkFdbConfig = {
    val cimp = new CaseInsensitiveStringMap(options.asJava)

    val layerProviderClass = Option(cimp.get(LAYER_PROVIDER_CLASS))
    val releaseDelayMs     = Option(cimp.get(RELEASE_DELAY_MS)).flatMap(_.toIntOption).getOrElse(1000)

    SparkFdbConfig(dbConfig, layerProviderClass, releaseDelayMs, options)

  }

  def fromMap(options: Map[String, String]): SparkFdbConfig = {
    val fdb = FoundationDbConfig.fromMap(options)

    fromMap(options, fdb)
  }
}
