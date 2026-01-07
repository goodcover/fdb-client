package org.apache.spark.sql.fdb.test

import com.goodcover.fdb.*
import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabaseFactory
import com.goodcover.fdb.record.SharedTestLayers
import com.goodcover.fdb.record.es.EventsourceLayer
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.fdb.FdbCatalog
import zio.stream.ZStream
import zio.test.*
import zio.{ Scope, ZIO }

object SparkCatalogSpark4Spec extends SharedZIOSparkSpecDefault {

  override def spec: Spec[SparkSession & TestEnvironment & Scope, Any] =
    (suite("SparkCatalogSpec")(
      test("should find metadata") {

        for {
          session <- ZIO.service[SparkSession]
          service <- ZIO.service[EventsourceLayer]
          _       <- service.saveMetadata
          _       <- service.loadMetadata.flatMap(m => ZIO.logDebug(s"Loaded metadata? $m"))
          _       <- ZStream
                       .range(0, 30)
                       .mapZIOPar(5) { i =>
                         val seqNr = i % 10
                         val id    = i / 10
                         val tagId = i / 15 + 1
                         SparkTableSpark4Spec.dumbAppend(
                           s"pid-$id",
                           seqNr.toLong,
                           s"tag$tagId" :: Nil,
                         )
                       }
                       .runCollect
          testId  <- SparkTableSpark4Spec.getTestValue
          cname    = "fdb1"
          cns      = "ns1"
          _       <- ZIO.attempt {
                       import org.apache.spark.sql.fdb.ReadConf.*
                       import org.apache.spark.sql.fdb.SparkFdbConfig.*
                       import FoundationDbConfig.*
                       val catalog = s"spark.sql.catalog.$cname"
                       val ns1Ns   = s"spark.sql.catalog.$cname.namespace.$cns"
                       session.conf
                       session.conf.set(catalog, FdbCatalog.CATALOG)
                       session.conf.set(s"$catalog.$LAYER_PROVIDER_CLASS", SparkTableSpark4Spec.layerCls)
                       session.conf.set(s"$ns1Ns.$KEYSPACE_PROVIDER_CLASS", SparkTableSpark4Spec.provideCls)
                       session.conf.set(s"$ns1Ns.testid", testId)

                       session.conf.set(s"$catalog.$SEARCH_CLUSTER_FILE", "true")

                     }

          result  <- ZIO.attempt {
                       session.read.table(s"$cname.$cns.PersistentRepr").count()
                     }
          result2 <- ZIO.attempt {
                       import session.implicits.*
                       session.read
                         .table(s"$cname.$cns.PersistentRepr")
                         .filter(
//                           "array_contains(tags, \"tag1\")"
                           // Above does not get pushed down
                           $"tags" === Array("tag1")
                         )
                         .count()
                     }
        } yield assertTrue(30L == result, 15L == result2)
      },
    ) @@ TestAspect.withLiveClock)
      .provideSome[FdbPool & SparkSession]
      .apply(
        FdbRecordDatabaseFactory.live >+>
          TestId.layer >+>
          SharedTestLayers.ConfigLayer >+>
          EventsourceLayer.live >+>
          SharedTestLayers.ClearAll
      )
      .provideSomeShared[SparkSession](
        FdbSpecLayers.sharedLayer
      )

}
