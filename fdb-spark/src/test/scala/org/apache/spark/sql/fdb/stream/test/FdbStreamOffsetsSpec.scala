package org.apache.spark.sql.fdb.stream.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.fdb.KeyBytes
import org.apache.spark.sql.fdb.stream.FdbStreamOffsets
import org.apache.spark.sql.fdb.test.SharedZIOSparkSpecDefault
import zio.Scope
import zio.test.*

object FdbStreamOffsetsSpec extends SharedZIOSparkSpecDefault {

  override def spec: Spec[SparkSession & TestEnvironment & Scope, Any] = suite("FdbStreamOffsetSpec")(
    test("test serialization") {
      import FdbStreamOffsets.*

      val start      =
        FdbStreamOffsets(
          0L,
          "testing",
          (0 to 20)
            .map(i =>
              i.toLong -> SingleContinuation(
                i,
                KeyBytes(Array(i.toByte, 2, 116, 97, 103, 50, 0)),
                KeyBytes(null)
              )
            )
        )
      val end        = start.json()
      val startPrime = FdbStreamOffsets.fromJson(end)

      assertTrue(
        KeyBytes(Array(0)) == KeyBytes(Array(0)),
        KeyBytes(Array(1)) == KeyBytes(Array(1)),
        startPrime.copy(seq = Seq.empty) == start.copy(seq = Seq.empty),
        startPrime.seq.toSeq.sortBy(_._1).map(_._2) == start.seq.toSeq.sortBy(_._1).map(_._2),
      )
    }
  )
}
