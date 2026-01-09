package org.apache.spark.sql.fdb

import org.apache.spark.sql.catalyst.DeserializerBuildHelper
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{ BinaryType, DataType, LongType, ObjectType }
import org.apache.spark.sql.{ Encoder, SparkSession }
import com.goodcover.fdb.record.es.proto.PersistentRepr
import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.expressions.objects.{ Invoke, StaticInvoke }
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

  given Deserializer[ByteString] with
    def inputType: DataType = BinaryType

    def deserialize(path: Expression): Expression =
      StaticInvoke(
        classOf[ByteString],
        ObjectType(classOf[ByteString]),
        "copyFrom",
        path :: Nil
      )

  given Serializer[ByteString] with
    def inputType: DataType = ObjectType(classOf[ByteString])

    def serialize(path: Expression): Expression =
      Invoke(path, "toByteArray", BinaryType, Seq.empty)

  val encoderA: Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])] =
    summon[Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])]]

  val encoderB: Encoder[java.lang.Long] =
    summon[Encoder[java.lang.Long]]

  val encoderPersistentRepr: Encoder[PersistentRepr] =
    summon[Encoder[PersistentRepr]]
}
