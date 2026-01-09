package org.apache.spark.sql.fdb.test

import com.apple.foundationdb.record.EndpointType
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType
import com.apple.foundationdb.record.provider.foundationdb.keyspace.{ KeySpace, KeySpaceDirectory, KeySpacePath }
import com.apple.foundationdb.record.query.expressions.{ Query, QueryComponent }
import com.goodcover.fdb.*
import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabaseFactory
import com.goodcover.fdb.record.SharedTestLayers
import com.goodcover.fdb.record.es.EventsourceLayer.EventsourceConfig
import com.goodcover.fdb.record.es.proto.PersistentRepr
import com.goodcover.fdb.record.es.{ EventsourceLayer, EventsourceMeta }
import com.google.protobuf.{ ByteString, Descriptors }
import org.apache.spark.sql.*
import org.apache.spark.sql.fdb.*
import org.apache.spark.sql.fdb.ReadConf.KeySpaceProvider
import zio.*
import zio.stream.ZStream
import zio.test.{ Spec, TestAspect, TestEnvironment, assertTrue }

import java.util.concurrent.TimeUnit

object SparkReaderSpec extends SharedZIOSparkSpecDefault {

  /** This annoyingly relies on the scheme from tests to create this. */
  private[test] class Provide(foo: Map[String, String]) extends KeySpaceProvider {

    private def base = foo("testid")

    private def ks = new KeySpaceDirectory("tests", KeyType.STRING, base)
      .addSubdirectory(new KeySpaceDirectory("table", KeyType.STRING, "t"))
      .addSubdirectory(new KeySpaceDirectory("meta", KeyType.STRING, "m"))

    override def keySpace: KeySpace = new KeySpace(ks)

    override def tablePath: KeySpacePath = keySpace.path("tests").add("table")

    override def metaPath: KeySpacePath = keySpace.path("tests").add("meta")

    override def fileDescriptor(): Descriptors.FileDescriptor = EventsourceMeta.descriptor
  }

  private[test] class LayerClass extends LayerProvider {

    override def get: ZLayer[Any, Nothing, Unit] =
      Runtime.removeDefaultLoggers ++
        zio.Runtime.addLogger(TestFdbLogger) ++
        Runtime.enableCurrentFiber
  }

  private[test] case class PartScheme(foo: Map[String, String]) extends PartitionScheme {
    private val uniqueTags = foo("uniquetags").split(",")

    override def scheme: PartitionScheme.SchemeType = PartitionScheme.QueryRestrictionEnum

    override def partitions(metaData: FdbTable.MetadataWith[ReadConf]): Array[InputPartitionScheme] =
      uniqueTags.map { tag =>
        new QueryRestrictionScheme {
          override def queryAnd: QueryComponent = Query.field("tags").oneOfThem().equalsValue(tag)
        }
      }
  }

  private[test] case class PrimaryPartScheme(foo: Map[String, String]) extends PartitionScheme {

    override def scheme: PartitionScheme.SchemeType = PartitionScheme.PrimaryRestrictionEnum

    override def partitions(metaData: FdbTable.MetadataWith[ReadConf]): Array[InputPartitionScheme] = {
      val s          = StorageHelper(metaData.dbConfig)
      val boundaries = s.unsafeRun {
        s.db.runAsync { rc =>
          for {
            store      <- rc.createOrOpen(metaData.recordMetaData)
            boundaries <- store.primaryKeyBoundaries(None).runCollect
          } yield boundaries
        }
      }

      println(boundaries)

      BaseScanner.partitionViaPrimaryKey(None, None, boundaries).map { _range =>
        new PrimaryRestrictionScheme {
          override def range: FdbSerializableRange = _range
        }
      }
    }
  }

  implicit class DefaultsDFR(dfr: DataFrameReader) {

    def defaults: DataFrameReader =
      dfr
        .format(FdbProvider.FDB_PLAIN)
        .option(ReadConf.RECORD_TYPE, "PersistentRepr")
        .option(FoundationDbConfig.SEARCH_CLUSTER_FILE, "true")
        .option(SparkFdbConfig.LAYER_PROVIDER_CLASS, layerCls)
  }

  implicit class DefaultsDFW[T](dfw: DataFrameWriter[T]) {

    def defaults: DataFrameWriter[T] =
      dfw
        .format(FdbProvider.FDB_PLAIN)
        .option(ReadConf.RECORD_TYPE, "PersistentRepr")
        .option(FoundationDbConfig.SEARCH_CLUSTER_FILE, "true")
        .option(SparkFdbConfig.LAYER_PROVIDER_CLASS, layerCls)
  }

  private[test] val provideCls           = getClass.getName + classOf[Provide].getSimpleName
  private[test] val layerCls             = getClass.getName + classOf[LayerClass].getSimpleName
  private[test] val partSchemeCls        = getClass.getName + classOf[PartScheme].getSimpleName
  private[test] val primaryPartSchemeCls = getClass.getName + classOf[PrimaryPartScheme].getSimpleName

  private[test] def dumbAppend(
    id: String,
    seqNr: Long,
    tags: Seq[String],
    payload: ByteString = ByteString.EMPTY,
  ): ZIO[EventsourceLayer, Throwable, PersistentRepr] =
    ZIO.serviceWithZIO[EventsourceLayer].apply { service =>
      Clock.currentTime(TimeUnit.MILLISECONDS).flatMap { ms =>
        val pr = PersistentRepr.of(id, seqNr, ms, tags, payload, None, None, Map.empty)
        service
          .appendEvents(Seq(pr))
          .as(pr)
      }
    }

  private[test] def getTestValue: ZIO[EventsourceConfig, Nothing, String] =
    ZIO.serviceWith[EventsourceConfig].apply(_.keyspace.getDirectory("tests").getValue.asInstanceOf[String])

  override def spec: Spec[SparkSession & TestEnvironment & Scope, Any] =
    (suite("SparkReaderSpec")(
      test("should use count when available") {
        val maxElements = 3000

        for {
          session      <- ZIO.service[SparkSession]
          service      <- ZIO.service[EventsourceLayer]
          _            <- service.saveMetadata
          _            <- service.loadMetadata.flatMap(m => ZIO.logDebug(s"Loaded metadata? $m"))
          payload      <- Random.nextBytes(20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))
          start        <- Clock.nanoTime
          (insertT, _) <-
            ZStream
              .range(0, maxElements)
              .mapZIOParUnordered(64) { i =>
                val seqNr = i % 10
                val id    = i / 10
                val tagId = i / 15 + 1
                val tag   = s"tag$tagId"
                dumbAppend(s"pid-$id", seqNr.toLong, tag :: Nil, payload = payload).flatMap { _ =>
                  ZIO.when(i % 10_000 == 0)(Clock.nanoTime.flatMap { now =>
                    val duration = Duration(now - start, TimeUnit.NANOSECONDS).toMillis.toDouble
                    val rps      = if (duration == 0 || i == 0) "" else f" (${(i / duration) * 1000}%.2f avg req/s)"
                    ZIO.logInfo(s"Inserted $i elements so far$rps")
                  })
                }
              }
              .runCollect
              .timed

          testId         <- getTestValue
          (readT, count) <- ZIO.attempt {
                              session.read
                                .option("testId", testId)
                                .defaults
                                .option(ReadConf.PARTITION_SCHEME_CLASS, primaryPartSchemeCls)
                                .load(provideCls)
                                .count()
                            }.timed

          _ <- ZIO.logInfo(s"Took $readT to read")
          _ <- ZIO.logInfo(
                 s"Took $insertT to insert $maxElements elements (${maxElements / Math.max(1, insertT.toSeconds)} inserts/sec)"
               )
        } yield assertTrue(count == maxElements)

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
      .provideSomeShared[SparkSession]
      .apply(FdbSpecLayers.sharedLayer)

}
