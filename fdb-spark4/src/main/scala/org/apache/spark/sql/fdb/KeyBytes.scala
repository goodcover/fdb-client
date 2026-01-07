package org.apache.spark.sql.fdb

import com.apple.foundationdb.tuple.ByteArrayUtil2
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*

import java.util

case class KeyBytes(key: Array[Byte]) extends Comparable[KeyBytes] {
  def isNull = key == null

  override def equals(obj: Any): Boolean = obj match {
    case k: KeyBytes => util.Arrays.compare(key, k.key) == 0
    case x           => x == this
  }

  override def compareTo(o: KeyBytes): Int = util.Arrays.compare(key, o.key)

  override def toString: String =
    if (isNull) "KeyBytes(null)"
    else
      s"KeyBytes(${ByteArrayUtil2.loggable(key)})"
}

object KeyBytes {

  implicit val formats: JsonValueCodec[KeyBytes] = new JsonValueCodec[KeyBytes] {

    override def nullValue: KeyBytes = KeyBytes(null)

    override def decodeValue(
      in: JsonReader,
      default: KeyBytes
    ): KeyBytes = {
      val bytes = in.readBase64AsBytes(Array.empty)
      if (bytes.isEmpty) nullValue
      else KeyBytes(bytes)
    }

    override def encodeValue(x: KeyBytes, out: JsonWriter): Unit =
      if (x.isNull) {
        out.writeBase64Val(Array.empty, false)
      } else
        out.writeBase64Val(x.key, false)
  }

}
