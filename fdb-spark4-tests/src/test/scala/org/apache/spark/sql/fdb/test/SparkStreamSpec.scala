package org.apache.spark.sql.fdb.test

import com.apple.foundationdb.record.provider.foundationdb.SplitHelper
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType
import com.apple.foundationdb.record.provider.foundationdb.keyspace.{ KeySpace, KeySpaceDirectory, KeySpacePath }
import com.apple.foundationdb.record.{ EndpointType, TupleRange }
import com.apple.foundationdb.tuple.Tuple
import com.goodcover.fdb.*
import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabaseFactory
import com.goodcover.fdb.record.SharedTestLayers
import com.goodcover.fdb.record.es.EventsourceLayer.EventsourceConfig
import com.goodcover.fdb.record.es.{ EventsourceLayer, EventsourceMeta }
import com.goodcover.fdb.record.es.proto.PersistentRepr
import com.google.protobuf.{ ByteString, Descriptors }
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{ Encoder, SparkSession }
import org.apache.spark.sql.fdb.{ FdbProvider, ReadConf, SparkFdbConfig, TestEncoderImpl }
import org.apache.spark.sql.fdb.ReadConf.KeySpaceProvider
import org.apache.spark.sql.fdb.stream.{ FdbIndexPartition, FdbMicrobatchProvider, MicrobatchConfig }
import org.apache.spark.sql.fdb.test.SparkStreamSpec.ZIOError
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.streaming.{ StreamTest, Trigger }
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import zio.stream.ZStream
import zio.{ Chunk, Exit, Random, Runtime, Trace, Unsafe, ZIO, ZLayer }

import java.nio.file.Paths
import scala.concurrent.Future

class SparkStreamSpec extends StreamTest with SharedSparkSession with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(600.seconds)

  override val streamingTimeout = 600.seconds

  /** This could probably be better wrapped about */
  protected def createSparkReadStream(microBatchSize: Int)(implicit
    spark: SparkSession
  ) =
    getTestValue.flatMap { testId =>
      ZIO.attempt {
        val reader = spark.readStream
          .format(FdbProvider.FDB_PLAIN)
          .option(SparkFdbConfig.LAYER_PROVIDER_CLASS, SparkStreamSpec.layerCls)
          .option(ReadConf.RECORD_TYPE, "PersistentRepr")
          .option("searchclusterfile", "true")
          .option(MicrobatchConfig.MICROBATCH_PROVIDER_CLASS, SparkStreamSpec.microbatchCls)
          .option(MicrobatchConfig.MICROBATCH_MAX_BATCH_SIZE, microBatchSize) // per partition
          // Next options are local to the configuration classes
          .option("testId", testId)
          // This just sets how many batches to look at
          .option("microbatchSize", microBatchSize)

        reader
      }
    }

  private val runtime = Runtime.default

  /**
   * This could probably be thought about better as well? What's the best way to
   * integrate with ZIO runtime from non-zio land? I run into this all the time.
   */
  private[test] def unsafeRunToFuture[E, A](effect: ZIO[Any, E, A])(implicit trace: Trace): Future[A] = {
    val promise = scala.concurrent.Promise[A]()
    Unsafe.unsafe { implicit unsafe =>
      val task = effect.mapError {
        case t: Throwable => t
        case t            => ZIOError(t)
      }

      runtime.unsafe.fork(task).unsafe.addObserver {
        case Exit.Success(res)   => promise.success(res)
        case Exit.Failure(cause) => promise.failure(cause.squash)
      }
    }
    promise.future
  }

  /**
   * Nice function to run the effect but also to pass in the per test layers.
   */
  protected def run[E, A](
    effect: ZIO[FdbRecordDatabaseFactory & TestId & EventsourceConfig & EventsourceLayer & SparkSession, E, A]
  )(implicit trace: Trace): A =
    unsafeRunToFuture(effect.provide(perTestLayer)).futureValue

  /** One layer used by all, shutdown at the end */
  private val sharedLayer = Unsafe.unsafe { implicit unsafe =>
    Runtime.unsafe.fromLayer(FdbSpecLayers.sharedLayer)
  }

  private def perTestLayer(implicit trace: Trace) = ZLayer.succeedEnvironment(sharedLayer.environment) >+>
    ZLayer.succeed(spark) >+>
    (FdbRecordDatabaseFactory.live >+>
      TestId.layer >+>
      SharedTestLayers.ConfigLayer >+>
      deleteCheckpointLayer() >+>
      EventsourceLayer.live >+>
      SharedTestLayers.ClearAll)

  /**
   * Instead of deleting the end we do the beginning so you can examine the
   * files
   */
  private def deleteCheckpointLayer(): ZLayer[EventsourceConfig, Nothing, Unit] = ZLayer.fromZIO(getTestValue.map { file =>
    FileUtils.deleteDirectory(Paths.get("target", file).toFile)
    ()
  })

  override def afterAll(): Unit = {
    sharedLayer.shutdown0()
    super.afterAll()
  }

  private[test] def getTestValue: ZIO[EventsourceConfig, Nothing, String] =
    ZIO.serviceWith[EventsourceConfig].apply(_.keyspace.getDirectory("tests").getValue.asInstanceOf[String])

  def getFirstNTimestamps(ch: zio.Chunk[PersistentRepr], batchSize: Int)(i: Int): Chunk[Long] =
    zio.Chunk.fromIterable(ch.groupBy(_.tags.head).flatMap { case (k, chunks) =>
      chunks.take(batchSize * i).map(_.timestamp)
    })

  test("should insert") {

    val clock = new StreamManualClock

    run {
      for {
        service      <- ZIO.service[EventsourceLayer]
        ss           <- ZIO.service[SparkSession]
        _            <- service.saveMetadata
        payload      <- Random.nextBytes(SplitHelper.SPLIT_RECORD_SIZE * 20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))
        // Insert tags in groups of eight
        toInsert     <- ZStream
                          .range(0, 24)
                          .mapZIO { i =>
                            val seqNr = i % 8 + 1
                            val id    = i / 8
                            val pid   = s"pid-$id"
                            val tags  = s"tag$id" :: Nil

                            val pr = PersistentRepr(
                              pid,
                              seqNr,
                              (i + 1).toLong,
                              tags,
                              payload,
                            )
                            service.appendEvents(pr).as(pr)

                          }
                          .runCollect
        size         <- ZIO.attempt(toInsert.flatMap(p => p.tags).distinct.length)
        testId       <- getTestValue
        checkpointDir = s"target/$testId"
        read         <- createSparkReadStream(microBatchSize = size)
        result       <- ZIO.attempt {
                          val enc = new TestEncoderImpl(spark)

                          val ds = read
                            .load(SparkStreamSpec.provideCls)

                          val toDisplay =
                            ds.map { row =>
                              (
                                row.getAs[java.lang.Long](2), // Timestamp
                                row.getAs[String](0),         // persistenceId
                                row.getAs[java.lang.Long](1), // SequenceNr
                                row.get(3).asInstanceOf[scala.collection.Seq[String]].toSeq
                              )
                            }(enc.encoderA)

                          ds.map(row => row.getAs[java.lang.Long](2))(enc.encoderB)
                        }
        getter        = getFirstNTimestamps(toInsert, size) _
        _            <- ZIO.attempt {
                          import ss.implicits.*

                          val map = Map("microbatchSize" -> size.toString)

                          testStream(result)(
                            StartStream(Trigger.ProcessingTime(100), clock, map, checkpointDir),
                            waitUntilBatch(clock),
                            CheckAnswer(getter(1)*),
                            AdvanceManualClock(100),
                            waitUntilBatch(clock),
                            CheckAnswer(getter(2)*),
                            AdvanceManualClock(100),
                            waitUntilBatch(clock),
                            CheckAnswer(getter(3)*),
                            AdvanceManualClock(100),
                            waitUntilBatch(clock),
                            CheckAnswer(getter(4)*),
                            StopStream,
                            StartStream(Trigger.ProcessingTime(100), clock, map, checkpointDir),
                            AdvanceManualClock(100),
                            waitUntilBatch(clock),
                            CheckAnswer(getter(5)*),
                          )
                        }
        fromSpark    <- service.currentEventsById(toInsert.head.persistenceId).runCollect
      } yield assert(fromSpark.size == 8)
    }
  }

  /**
   * Pulled from kinesis, a solid example of how to build a
   * non-in-spark-repo-tree connector
   *
   * https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamSuite.scala
   */
  def waitUntilBatch(clock: StreamManualClock): AssertOnQuery =
    Execute { q =>
      logInfo(s"waitUntilBatchProcessed start with timeout $streamingTimeout")
      Thread.sleep(50L)
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        logInfo(s"waitUntilBatchProcessed ended with exception.", q.exception.get)
        throw q.exception.get
      }
      logInfo(s"waitUntilBatchProcessed ended")
    }

}

object SparkStreamSpec {

  /** This annoyingly relies on the scheme from tests to create this. */
  private[test] class Provide(options: Map[String, String]) extends KeySpaceProvider {

    private def base = options("testid")

    private def ks = new KeySpaceDirectory("tests", KeyType.STRING, base)
      .addSubdirectory(new KeySpaceDirectory("table", KeyType.STRING, "t"))
      .addSubdirectory(new KeySpaceDirectory("meta", KeyType.STRING, "m"))

    override def keySpace: KeySpace = new KeySpace(ks)

    override def tablePath: KeySpacePath = keySpace.path("tests").add("table")

    override def metaPath: KeySpacePath = keySpace.path("tests").add("meta")

    override def fileDescriptor(): Descriptors.FileDescriptor = EventsourceMeta.descriptor
  }

  /**
   * This relies on solely the scheme in the `should insert` test to spit out
   * the right amount of tags/tag-scheme
   */
  private[test] class MicrobatchProvider(options: Map[String, String]) extends FdbMicrobatchProvider[FdbIndexPartition] {

    private val size = options("microbatchsize").toInt

    override def partitions: Array[FdbIndexPartition] = Array.tabulate(size) { i =>
      val tag = s"tag$i"
      new FdbIndexPartition {

        override def shardId: Long = i

        override def indexTuple: TupleRange = new TupleRange(
          Tuple.from(tag),
          Tuple.from(tag),
          EndpointType.RANGE_INCLUSIVE,
          EndpointType.TREE_END
        )

        override def indexName: String = "eventTagIndex"
      }
    }
  }

  private[test] class LayerClass extends LayerProvider {
    override def get: ZLayer[Any, Nothing, Unit] =
      Runtime.removeDefaultLoggers ++
        zio.Runtime.addLogger(TestFdbLogger) ++
        Runtime.enableCurrentFiber
  }

  private[test] val provideCls    = getClass.getName + classOf[Provide].getSimpleName
  private[test] val layerCls      = getClass.getName + classOf[LayerClass].getSimpleName
  private[test] val microbatchCls = getClass.getName + classOf[MicrobatchProvider].getSimpleName

  case class ZIOError[E](e: E) extends Throwable
}
