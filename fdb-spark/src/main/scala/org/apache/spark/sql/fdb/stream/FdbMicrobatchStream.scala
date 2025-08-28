package org.apache.spark.sql.fdb.stream

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming.{ MicroBatchStream, Offset, ReadLimit, SupportsAdmissionControl }
import org.apache.spark.sql.connector.read.{ InputPartition, PartitionReader, PartitionReaderFactory }
import org.apache.spark.sql.fdb.{ FdbSerializableRange, FdbTable, KeyBytes }
import org.apache.spark.sql.fdb.metadata.HDFSMetadataCommitter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{ SerializableConfiguration, ShutdownHookManager }
import org.apache.spark.util.ThreadUtils.newDaemonSingleThreadScheduledExecutor

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ ScheduledFuture, TimeUnit }
import scala.collection.immutable.ArraySeq
import scala.util.Try
import scala.util.control.NonFatal

class FdbMicrobatchStream(
  schema: StructType,
  conf: FdbTable.MetadataWith[MicrobatchConfig],
  checkpointLocation: String,
  provider: FdbMicrobatchProvider[FdbIndexPartition]
) extends SupportsAdmissionControl
    with Logging
    with MicroBatchStream { self =>

  private lazy val generatedStreamName = provider.partitions.head.indexName
  private val streamName               = conf.config.microbatchStreamName.getOrElse(generatedStreamName)
  private val sqlConf                  = SQLConf.get

  private val minBatchesToRetain           = sqlConf.minBatchesToRetain
  private val streamingMaintenanceInterval = sqlConf.streamingMaintenanceInterval

  private var currentShardOffsets: Option[FdbStreamOffsets] = None

  private val hadoopConf = new SerializableConfiguration(SparkSession.getActiveSession.map { s =>
    logInfo(s"FdbMicrobatchStream use sparkContext.hadoopConfiguration")
    s.sparkContext.hadoopConfiguration
  }.getOrElse {
    logInfo(s"FdbMicrobatchStream create default Configuration")
    new Configuration()
  })

  val metadataCommitter =
    new HDFSMetadataCommitter[FdbStreamOffsets.SingleContinuation](checkpointLocation, hadoopConf)

  private var maintenanceTask: Option[MaintenanceTask] = None
  private val shuttingDown                             = new AtomicBoolean(false)

  private def getBatchShardsInfo(batchId: Long): Map[Long, FdbStreamOffsets.SingleContinuation] =
    if (batchId < 0) {
      logInfo(s"This is the first batch. Returning Empty sequence")
      initialPartitions.toMap
    } else {
      if (metadataCommitter.exists(batchId)) {
        logInfo(s"getBatchShardsInfo for batchId $batchId")
        val shardsInfo = metadataCommitter.get(batchId)
        logDebug(s"Shard Info is $shardsInfo")
        shardsInfo.map { i =>
          i.shardId -> i
        }.toMap
      } else {
        logInfo(s"getBatchShardsInfo for batchId $batchId doesn't exists")
        Map.empty
      }

    }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    logDebug(s"get latestOffset with start Offset $start")
    val prevOffset     = start.asInstanceOf[FdbStreamOffsets]
    val prevBatchId    = prevOffset.batchId
    logInfo(s"get latestOffset prevBatchId $prevBatchId")
    val prevShardsInfo = getBatchShardsInfo(prevBatchId)
    logDebug(s"get latestOffset prevShardsInfo $prevShardsInfo")

    if (prevBatchId >= 0 && prevShardsInfo.isEmpty) {
      // Attempt to get the one right before this, in case something got cut off
      logError(
        s"Unable to fetch " +
          s"committed metadata from previous batch id $prevBatchId. Some data may have been missed"
      )

      val newPrevBatchId = prevBatchId - 1

      val prevShardsInfo = getBatchShardsInfo(newPrevBatchId)

      if (newPrevBatchId >= 0 && prevShardsInfo.isEmpty) {
        throw new IllegalStateException(
          s"Unable to fetch " +
            s"committed metadata from previous batch id $newPrevBatchId. Some data may have been missed"
        )
      } else if (shuttingDown.get()) null
      else {
        // In this case we just skip this batchId, I think there's some sort of write error, we need
        // to move on to the next batch though.
        val newOffsets = new FdbStreamOffsets(prevBatchId + 1, streamName, prevShardsInfo.toSeq)

        currentShardOffsets = Some(newOffsets)
        newOffsets
      }
    } else if (shuttingDown.get()) {
      null
    } else {
      val newOffsets = new FdbStreamOffsets(prevBatchId + 1, streamName, prevShardsInfo.toSeq)

      currentShardOffsets = Some(newOffsets)
      newOffsets
    }
  }

  override def latestOffset(): Offset =
    throw new Exception("Don't use")

  override def reportLatestOffset(): Offset =
    currentShardOffsets match {
      case None      => initialOffset()
      case Some(cso) => cso
    }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    logInfo(s"planInputPartitions currentBatch $end")
    val currBatch         = end.asInstanceOf[FdbStreamOffsets].seq.toMap
    val currBatchId       = end.asInstanceOf[FdbStreamOffsets].batchId
    val prevBatchId: Long = if (start != null) {
      start.asInstanceOf[FdbStreamOffsets].batchId
    } else {
      -1.toLong
    }

    logInfo(s"prevBatchId $prevBatchId, currBatchId $currBatchId")
    assert(prevBatchId <= currBatchId)

    provider.partitions.map { case p =>
      FdbMicrobatchInputPartition(
        indexName = p.indexName,
        batchId = currBatchId,
        shardId = p.shardId,
        range = FdbSerializableRange(
          p.indexTuple.getLow.pack(),
          p.indexTuple.getHigh.pack(),
          p.indexTuple.getLowEndpoint,
          p.indexTuple.getHighEndpoint,
          currBatch.get(p.shardId).fold[KeyBytes](KeyBytes(null))(_.continuation)
        )
      )

    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new PartitionReaderFactory {
    private val tableConfig = conf.config
    private val dbConfig    = conf.dbConfig
    private val schemaS     = schema
    private val cl          = checkpointLocation
    private val hc          = hadoopConf
    private val batchId     = currentShardOffsets.map(_.batchId)

    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
      val fdbPartition =
        partition.asInstanceOf[FdbMicrobatchInputPartition]

      new FdbPartitionReader( //
        dbConfig,
        tableConfig,
        cl,
        hc,
        batchId.getOrElse(-1L),
        fdbPartition.shardId,
        schemaS,
        fdbPartition
      )
    }
  }

  private def initialPartitions: Array[(Long, FdbStreamOffsets.SingleContinuation)] = provider.partitions.map { p =>
    val low = p.indexTuple.getLow.pack()
    p.shardId -> FdbStreamOffsets.SingleContinuation(p.shardId, KeyBytes(low), KeyBytes(null))
  }

  override def initialOffset(): Offset =
    FdbStreamOffsets(
      -1L,
      streamName,
      ArraySeq.unsafeWrapArray(initialPartitions)
    )

  override def deserializeOffset(json: String): Offset =
    FdbStreamOffsets.fromJson(json)

  override def commit(end: Offset): Unit =
    if (maintenanceTask.isEmpty || !maintenanceTask.get.isRunning) {
      stopMaintenance()
      maintenanceTask = Some(
        new MaintenanceTask(
          streamingMaintenanceInterval,
          task = doMaintenance(),
          onError = logWarning("Metadata maintenance task got an error")
        )
      )
    }

  override def stop(): Unit = {
    logInfo("Calling stop")
    stopMaintenance()

  }

  private def stopMaintenance(): Unit =
    Try(maintenanceTask.foreach(_.stop())).fold(
      _ => logWarning("Failed to stop maintenanceTask"),
      _ => maintenanceTask = None
    )

  private def doMaintenance(): Unit = {
    logInfo("Metadata maintenance task started")

    try
      metadataCommitter.purge(minBatchesToRetain)
    catch {
      case NonFatal(e) =>
        logWarning("Got error while do maintenance. Ignore it.", e)
    }

  }

  private class MaintenanceTask(periodMs: Long, task: => Unit, onError: => Unit) {
    self.logInfo(s"create MaintenanceTask with periodMs $periodMs")

    private val executor =
      newDaemonSingleThreadScheduledExecutor("FdbMicrobatchStream-maintenance-task")

    private val runnable = new Runnable {
      override def run(): Unit =
        try
          task
        catch {
          case NonFatal(e) =>
            logWarning("Error running maintenance thread", e)
            onError
        }
    }

    private val future: ScheduledFuture[?] = executor.scheduleAtFixedRate(runnable, periodMs, periodMs, TimeUnit.MILLISECONDS)

    def stop(): Unit = {
      future.cancel(true)
      executor.shutdown()
    }

    def isRunning: Boolean = !future.isDone
  }

  ShutdownHookManager.addShutdownHook { () =>
    logInfo("Shutting down, setting the bool to true")
    shuttingDown.set(true)
  }

}
