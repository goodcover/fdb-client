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

  case class IdAndTag(idInt: Ref[Int], tagInt: Ref[Int]) {
    def newTag: UIO[String] =
      tagInt.getAndUpdate(_ + 1).map(i => f"genTag$i%07d")

    def newId: UIO[String] =
      idInt.getAndUpdate(_ + 1).map(id => f"genPId$id%07d")

    def newIds(n: Int): UIO[List[String]] =
      ZIO.foreach((0 until n).toList)(_ => newId)

    def newTags(n: Int): UIO[List[String]] =
      ZIO.foreach((0 until n).toList)(_ => newTag)

  }

  val idTagGenLayer: ZLayer[Any, Nothing, IdAndTag] = ZLayer.fromZIO {
    Ref.make(0).zip(Ref.make(0)).map { case (p, t) =>
      IdAndTag(p, t)
    }
  }

  override val bootstrap: TaskLayer[SparkSession] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attempt(ss.getOrCreate()))(spark => ZIO.attempt(spark.stop()).orDie)
    }

}
