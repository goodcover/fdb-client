package org.apache.spark.sql.fdb.stream

import com.apple.foundationdb.record.*
import com.apple.foundationdb.record.provider.foundationdb.{ FDBIndexedRecord, IndexOrphanBehavior, IndexScanRange }
import com.apple.foundationdb.tuple.Tuple
import com.goodcover.fdb.record.RecordDatabase.{ FdbRecordContext, FdbRecordStore }
import com.google.protobuf.{ Descriptors, Message }
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.fdb.metadata.HDFSMetadataCommitter
import org.apache.spark.sql.fdb.{ KeyBytes, SparkFdbConfig, StorageHelper }
import org.apache.spark.sql.proto.ProtobufDeserializer
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import zio.stream.ZStream
import zio.{ ZIO, ZLayer }

import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }
import scala.annotation.unused
import scala.util.chaining.scalaUtilChainingOps

/**
 * So this is more advanced than the version with Cassandra, here's why
 *
 * 1) We don't even remotely care about time tracking so this is generic and
 * nice
 *
 * 2) We only do index scans by default, and there's no notion of a watch key so
 * we just poll, this is kinda stupid, but works similar
 *
 * 3) We use this technique adopted from aws kinesis connector to hook into the
 * lifecycle of the task and serialize our progress that way. We use
 * continuations so everything is supported by the underlying record layer
 */
class FdbPartitionReader(
  cfg: SparkFdbConfig,
  mbConfig: MicrobatchConfig,
  checkpointLocation: String,
  hadoopConf: SerializableConfiguration,
  batchId: Long,
  shardId: Long,
  schema: StructType,
  partition: FdbMicrobatchInputPartition
) extends PartitionReader[InternalRow]
    with Logging {
  private val storage: StorageHelper = StorageHelper(cfg)

  logInfo(
    s"FdbPartitionReader for batch ${partition.batchId} with " +
      s"sourcePartition start - ${partition.range}"
  )

  private val meta = storage.unsafeMetaData(mbConfig.readConf)

  val recordDescriptor: Descriptors.Descriptor = meta.recordType(mbConfig.readConf.recordType).getDescriptor
  val closed: AtomicBoolean                    = new AtomicBoolean(false)

  // We always want to emit 0's
  val sql = new ProtobufDeserializer(recordDescriptor, schema, emitDefaultValues = true)

  val metadataCommitter =
    new HDFSMetadataCommitter[FdbStreamOffsets.SingleContinuation](checkpointLocation, hadoopConf)

  private[this] var count = 0

  private[this] val errorRef: AtomicReference[Throwable]           = new AtomicReference[Throwable]
  private[this] val lastContinuation: AtomicReference[Array[Byte]] = new AtomicReference[Array[Byte]]

  private[this] val maxBatchSize = mbConfig.microbatchMaxBatchSize.getOrElse(1000)
  private[this] val useSnapshot  = mbConfig.microbatchSnapshot.getOrElse(false)

  private[this] val executionProperties =
    ExecuteProperties.newBuilder.tap { builder =>
      if (maxBatchSize <= 0)
        builder.setDefaultCursorStreamingMode(CursorStreamingMode.ITERATOR)
      else
        builder
          .setDefaultCursorStreamingMode(CursorStreamingMode.MEDIUM)
          .setScannedRecordsLimit(maxBatchSize)
    }
      .setIsolationLevel(if (useSnapshot) IsolationLevel.SNAPSHOT else IsolationLevel.SERIALIZABLE)
      .build

  private val tupleRange = new TupleRange( //
    Tuple.fromBytes(partition.range.low),
    Tuple.fromBytes(partition.range.high),
    partition.range.lowEndpoint,
    partition.range.highEndpoint
  )

  private val defaultContinuation = partition.range.continuation.key

  private val result: ZStream[Any, Throwable, FdbRecordStore.Continuable[FDBIndexedRecord[Message]]] =
    storage.db.streamAsyncContinuation { continue =>
      ZStream.unwrap {
        for {
          rc         <- ZIO.service[FdbRecordContext]
          _          <- ZIO.logInfo(
                          s"Creating FdRecordStore at the location ${mbConfig.readConf.tableKeySpacePath}, max batch size $maxBatchSize"
                        )
          tableStore <- rc.createOrOpen(meta)
        } yield {
          val usedContinuation = if (continue == null) defaultContinuation else continue
          tableStore.scanIndexRemoteFetch( //
            _.getIndex(partition.indexName),
            new IndexScanRange(IndexScanType.BY_VALUE, tupleRange),
            usedContinuation,
            new ScanProperties(
              executionProperties,
              false
            ),
            IndexOrphanBehavior.SKIP
          )

        }
      }
    }

  private val iterator = storage.unsafeRunBlocking {
    result.toIterator.provide(ZLayer.succeed(storage.scope))
  }

  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = iterator.next() match {
    case Left(value)  =>
      storage.shutdown()
      errorRef.set(value)
      throw value
    case Right(value) =>
      lastContinuation.set(value.continuation.toBytes)
      count += 1
      val result = sql.deserialize(value.record.getRecord).orNull
      result
  }

  override def close(): Unit = {
    logInfo(s"Start to close the partition $partition current value of closed=$closed")
    if (closed.compareAndSet(false, true)) {
      storage.shutdown()
      logWarning(s"Closed the partition, read=[$count], partition=$partition")
    }
  }

  private def updateMetadata(@unused taskContext: TaskContext): Unit = {
    // We use the range printable bytes for the "shardId" which corresponds to each row
    // partition you pass in.
    val lastContinue  = lastContinuation.get()
    val savedContinue = if (lastContinue == null) {
      // We didn't make any progress so use the last continuation
      partition.range.continuation
    } else {
      KeyBytes(lastContinue)
    }
    val shardInfo     = FdbStreamOffsets.SingleContinuation(shardId, KeyBytes(partition.range.low), savedContinue)

    logInfo(s"Batch $batchId : metadataCommitter adding shard position for $shardId, shardInfo $shardInfo")

    metadataCommitter.add(batchId, shardId.toString, shardInfo): Unit
  }

  TaskContext.get().addTaskCompletionListener[Unit] { (taskContext: TaskContext) =>
    logInfo(
      s"Complete Task for taskAttemptId ${taskContext.taskAttemptId()}, partitionId ${taskContext.partitionId()}, read $count rows"
    )
    val throwable = errorRef.get()
    if (throwable == null) {
      updateMetadata(taskContext)

    } else {
      logError("skip updateMetadata as stopped with error")
    }
  }: Unit
}
