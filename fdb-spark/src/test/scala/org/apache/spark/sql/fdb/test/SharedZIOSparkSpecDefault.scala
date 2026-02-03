package org.apache.spark.sql.fdb.test

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import zio.*
import zio.test.*

abstract class SharedZIOSparkSpecDefault extends ZIOSpec[SparkSession] {
  lazy val ss: SparkSession.Builder =
    SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.ui.enabled", value = false)
      .config("spark.serializer", classOf[KryoSerializer].getName)

  override val bootstrap: TaskLayer[SparkSession] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attempt(ss.getOrCreate()))(spark => ZIO.attempt(spark.stop()).orDie)
    }

}
