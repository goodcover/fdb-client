package org.apache.spark.sql.fdb

import com.goodcover.fdb.record.es.proto.PersistentRepr
import frameless.TypedExpressionEncoder

import org.apache.spark.sql.{ Encoder, Encoders, SparkSession }
import scalapb.spark.ProtoSQL

class TestEncoderImpl(spark: SparkSession) {
  import spark.implicits._

  val encoderA: Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])] =
    implicitly[Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])]]

  val encoderB: Encoder[java.lang.Long] =
    implicitly[Encoder[java.lang.Long]]

  val encoderPersistentRepr: Encoder[PersistentRepr] =
    TypedExpressionEncoder(ProtoSQL.implicits.messageTypedEncoder[PersistentRepr])
}
