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
import com.goodcover.fdb.record.es.{ EventsourceLayer, EventsourceMeta }
import com.goodcover.fdb.record.es.proto.PersistentRepr
import com.google.protobuf.{ ByteString, Descriptors }
import org.apache.spark.sql.fdb.{
  BaseScanner,
  FdbProvider,
  FdbSerializableRange,
  FdbTable,
  InputPartitionScheme,
  PartitionScheme,
  PrimaryRestrictionScheme,
  QueryRestrictionScheme,
  ReadConf,
  SparkFdbConfig,
  StorageHelper,
  TestEncoderImpl
}
import org.apache.spark.sql.fdb.ReadConf.KeySpaceProvider
import org.apache.spark.sql.{ DataFrameReader, DataFrameWriter, SaveMode, SparkSession }
import zio.*
import zio.stream.ZStream
import zio.test.{ Spec, TestAspect, TestEnvironment, assertTrue }

import java.util.concurrent.TimeUnit

object SparkTableSpec extends SharedZIOSparkSpecDefault {

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
    (suite("SparkTableSpec")(
      test("should test complex operators") {

        val numIds  = 6
        val numTags = 3
        val size    = 30
        val groups  = size / numIds
        val tags    = size / numTags
        for {
          session <- ZIO.service[SparkSession]
          service <- ZIO.service[EventsourceLayer]
          _       <- service.saveMetadata
          _       <- service.loadMetadata.flatMap(m => ZIO.logDebug(s"Loaded metadata? $m"))
          payload <- Random.nextBytes(1).map(bytes => ByteString.copyFrom(bytes.toArray))
          _       <-
            ZStream
              .range(0, size)
              .mapZIOPar(5) { i =>
                val seqNr = i % groups
                val id    = i / groups
                val tagId = i / tags + 1
                dumbAppend(s"pid-$id", seqNr.toLong, s"tag$tagId" :: Nil, payload = payload)
              }
              .runCollect

          testId <- getTestValue
          df      =
            session.read.defaults
              .option("testId", testId)
              .load(provideCls)

          _ <- assertTrue {
                 import session.implicits.*

                 df
                   .filter($"tags" === Array("tag1", "tag2")) // Poor man's oneOf
                   .count() == tags * 2
               }

          _ <- assertTrue {
                 import session.implicits.*

                 df
                   .filter($"tags" === Array("tag1", "tag2", "tag3") && $"sequenceNr" > (size / numIds / 2))
                   .count() == size / 2 - numTags
               }
          _ <- assertTrue {
                 import session.implicits.*

                 df
                   .filter($"sequenceNr" > 0)
                   .count() == size - numIds
               }
          _ <- assertTrue {
                 import session.implicits.*

                 df
                   .filter($"persistenceId" === "pid-0" && $"sequenceNr" >= 0)
                   .count() == (size / numIds)
               }
        } yield assertTrue(true)
      },
      test("should find metadata") {

        for {
          session    <- ZIO.service[SparkSession]
          service    <- ZIO.service[EventsourceLayer]
          _          <- service.saveMetadata
          _          <- service.loadMetadata.flatMap(m => ZIO.logDebug(s"Loaded metadata? $m"))
          payload    <-
            Random.nextBytes(SplitHelper.SPLIT_RECORD_SIZE * 20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))
          _          <-
            ZStream
              .range(0, 45)
              .mapZIOPar(5) { i =>
                val seqNr = i % 10
                val id    = i / 10
                val tagId = i / 15 + 1
                dumbAppend(s"pid-$id", seqNr.toLong, s"tag$tagId" :: Nil, payload = payload)
              }
              .runCollect
          testId     <- getTestValue
          resultHalf <- ZIO.attempt {
                          import session.implicits.*
                          session.read.defaults
                            .option("testId", testId)
                            .load(provideCls)
                            .filter($"tags" === Array("tag1"))
                            .count()
                        }

          resultAll <- ZIO.attempt {
                         import session.implicits.*
                         session.read.defaults
                           .option("testId", testId)
                           .load(provideCls)
                           .filter($"tags".apply(0).isin(Array("tag2", "tag1")*))
                           .count()
                       }
        } yield assertTrue( //
          15L == resultHalf,
          30L == resultAll,
        )
      },
      test("should insert") {

        for {
          session   <- ZIO.service[SparkSession]
          service   <- ZIO.service[EventsourceLayer]
          _         <- service.saveMetadata
          _         <- service.loadMetadata.flatMap(m => ZIO.logDebug(s"Loaded metadata? $m"))
          payload   <-
            Random.nextBytes(SplitHelper.SPLIT_RECORD_SIZE * 20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))
          toInsert  <-
            ZStream
              .range(0, 30)
              .mapZIO { i =>
                val seqNr = i % 10
                val id    = i / 10
                val tagId = i / 15 + 1
                Clock.currentTime(TimeUnit.MILLISECONDS).map { ms =>
                  PersistentRepr(s"pid-$id", seqNr, ms, s"tag$tagId" :: Nil, payload)
                }
              }
              .runCollect
          testId    <- getTestValue
          _         <- ZIO.attempt {
                         val enc = new TestEncoderImpl(session)

                         session
                           .createDataset(toInsert.toSeq)(enc.encoderPersistentRepr)
                           .repartition(1)
                           .write
                           .defaults
                           .option("testId", testId)
                           .option("batchedInserts", 2)
//                           .option("maxRequestsPerSecond", "1024")
                           .mode(SaveMode.Append)
                           .save(provideCls)
                       }
          fromSpark <- service.currentEventsById(toInsert.head.persistenceId).runCollect
        } yield assertTrue(fromSpark.size == 10)
      },
      test("should insert parallel 2") {

        for {
          session  <- ZIO.service[SparkSession]
          service  <- ZIO.service[EventsourceLayer]
          _        <- service.saveMetadata
          _        <- service.loadMetadata.flatMap(m => ZIO.logDebug(s"Loaded metadata? $m"))
          payload  <- Random.nextBytes(20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))
          toInsert <-
            ZStream
              .range(0, 1000)
              .mapZIO { i =>
                val seqNr = i % 10
                val id    = i / 10
                val tagId = i / 15 + 1
                Clock.currentTime(TimeUnit.MILLISECONDS).map { ms =>
                  PersistentRepr(s"pid-$id", seqNr, ms, s"tag$tagId" :: Nil, payload)
                }
              }
              .runCollect
          testId   <- getTestValue
          timed    <- ZIO.attempt {
                        val enc = new TestEncoderImpl(session)

                        session
                          .createDataset(toInsert.toSeq)(enc.encoderPersistentRepr)
                          .repartition(1)
                          .write
                          .defaults
                          .option("testId", testId)
                          .option("batchedInserts", 2)
                          .option("parallelBatches", 16)
                          //                           .option("maxRequestsPerSecond", "1024")
                          .mode(SaveMode.Append)
                          .save(provideCls)
                      }.timed

          _         <- ZIO.logInfo(s"Took $timed to insert")
          fromSpark <- service.currentEventsById(toInsert.head.persistenceId).runCollect
        } yield assertTrue(fromSpark.size == 10)
      },
      test("should pushdown count") {

        for {
          session        <- ZIO.service[SparkSession]
          service        <- ZIO.service[EventsourceLayer]
          _              <- service.saveMetadata
          _              <- service.loadMetadata.flatMap(m => ZIO.logDebug(s"Loaded metadata? $m"))
          payload        <- Random.nextBytes(20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))
          _              <-
            ZStream
              .range(0, 1000)
              .mapZIO { i =>
                val seqNr = i % 10
                val id    = i / 10
                val tagId = i / 15 + 1
                dumbAppend(s"pid-$id", seqNr.toLong, s"tag$tagId" :: Nil, payload = payload)
              }
              .runCollect
          testId         <- getTestValue
          (timed, count) <- ZIO.attempt {
                              session.read.defaults
                                .option("testId", testId)
                                .load(provideCls)
                                .count()
                            }.timed

          _ <- ZIO.logInfo(s"Took $timed to insert")
        } yield assertTrue(count == 1000)

      },
      test("should respect partition scheme") {

        for {
          session        <- ZIO.service[SparkSession]
          service        <- ZIO.service[EventsourceLayer]
          _              <- service.saveMetadata
          _              <- service.loadMetadata.flatMap(m => ZIO.logDebug(s"Loaded metadata? $m"))
          payload        <- Random.nextBytes(20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))
          uniqueTags     <-
            ZStream
              .range(0, 1000)
              .mapZIO { i =>
                val seqNr = i % 10
                val id    = i / 10
                val tagId = i / 15 + 1
                val tag   = s"tag$tagId"
                dumbAppend(s"pid-$id", seqNr.toLong, tag :: Nil, payload = payload).as(tag)
              }
              .runCollect
              .map(_.distinct)
          testId         <- getTestValue
          (timed, count) <- ZIO.attempt {
                              session.read
                                .option("testId", testId)
                                .option("uniqueTags", uniqueTags.mkString(",")) // LOOK ABOVE
                                .defaults
                                .option(ReadConf.PARTITION_SCHEME_CLASS, partSchemeCls)
                                .load(provideCls)
                                .count()
                            }.timed

          _ <- ZIO.logInfo(s"Took $timed to insert")
        } yield assertTrue(count == 1000)

      },
      test("should respect primary partition scheme") {
        val maxElements = 300_000

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
                 s"Took $insertT to insert $maxElements elements (${maxElements / insertT.toSeconds} inserts/sec)"
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
