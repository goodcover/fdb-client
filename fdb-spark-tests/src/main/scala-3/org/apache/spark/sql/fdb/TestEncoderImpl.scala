package org.apache.spark.sql.fdb

import org.apache.spark.sql.catalyst.DeserializerBuildHelper
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{ DataType, LongType }
import org.apache.spark.sql.{ Encoder, SparkSession }
import scala3encoders.derivation.{ Deserializer, Serializer }
import scala3encoders.given

class TestEncoderImpl(spark: SparkSession) {
  given Serializer[java.lang.Long] with
    def inputType: DataType = LongType

    def serialize(inputObject: Expression): Expression = inputObject

  given Deserializer[java.lang.Long] with
    def inputType: DataType = LongType

    def deserialize(path: Expression): Expression =
      DeserializerBuildHelper.createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Long])

  val encoderA: Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])] =
    summon[Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])]]

  val encoderB: Encoder[java.lang.Long] =
    summon[Encoder[java.lang.Long]]
}
