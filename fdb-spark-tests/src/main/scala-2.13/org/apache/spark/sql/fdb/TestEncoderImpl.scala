package org.apache.spark.sql.fdb

import org.apache.spark.sql.{ Encoder, Encoders, SparkSession }

class TestEncoderImpl(spark: SparkSession) {
  import spark.implicits._

  val encoderA: Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])] =
    implicitly[Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])]]

  val encoderB: Encoder[java.lang.Long] =
    implicitly[Encoder[java.lang.Long]]

}
