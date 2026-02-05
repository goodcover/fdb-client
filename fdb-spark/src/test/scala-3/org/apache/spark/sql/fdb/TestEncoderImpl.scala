package org.apache.spark.sql.fdb

import org.apache.spark.sql.catalyst.DeserializerBuildHelper
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{ BinaryType, DataType, LongType, ObjectType }
import org.apache.spark.sql.{ Encoder, SparkSession }
import com.goodcover.fdb.record.es.proto.PersistentRepr
import com.google.protobuf.ByteString
import io.github.pashashiz.spark_encoders.{ PrimitiveEncoder, TypedEncoder }
import io.github.pashashiz.spark_encoders.TypedEncoder.given

class TestEncoderImpl(spark: SparkSession) {

  implicit object julLongEncoder extends PrimitiveEncoder[java.lang.Long](LongType)

  val encoderA: Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])] =
    TypedEncoder.derive[(java.lang.Long, String, java.lang.Long, Seq[String])].encoder

  val encoderB: Encoder[java.lang.Long] = julLongEncoder.encoder

  implicit val byteStringEncoder: TypedEncoder[ByteString] =
    TypedEncoder.xmap[ByteString, Array[Byte]](_.toByteArray)(ByteString.copyFrom)

  val encoderPersistentRepr: Encoder[PersistentRepr] =
    TypedEncoder.derive[PersistentRepr].encoder
}
