package org.apache.spark.sql.fdb

import com.goodcover.fdb.record.es.proto.PersistentRepr
import com.google.protobuf.ByteString
import io.github.pashashiz.spark_encoders.TypedEncoder
import org.apache.spark.sql.{ Encoder, Encoders, SparkSession }
import io.github.pashashiz.spark_encoders.TypedEncoder.*

class TestEncoderImpl(spark: SparkSession) {
  import spark.implicits._

  val encoderA: Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])] =
    implicitly[Encoder[(java.lang.Long, String, java.lang.Long, Seq[String])]]

  val encoderB: Encoder[java.lang.Long] =
    implicitly[Encoder[java.lang.Long]]

  implicit val byteStringEncoder: TypedEncoder[ByteString] =
    TypedEncoder.xmap[ByteString, Array[Byte]](_.toByteArray)(ByteString.copyFrom)

  val encoderPersistentRepr: Encoder[PersistentRepr] =
    TypedEncoder.derive[PersistentRepr].encoder
}
