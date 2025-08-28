package org.apache.spark.sql.fdb

import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord
import com.apple.foundationdb.record.{ CursorStreamingMode, ExecuteProperties, IsolationLevel, ScanProperties }
import com.goodcover.fdb.record.RecordDatabase.{ FdbRecordContext, FdbRecordStore }
import com.google.protobuf.Message
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.proto.ProtobufDeserializer
import zio.stream.ZStream
import zio.{ Chunk, ZIO, ZLayer }

import java.util.concurrent.atomic.AtomicLong

/** Most of the heavy lifting is here */
class FdbScanPrimaryKeyPartitionReader(
  partition: FdbInputPartition,
  config: SparkFdbConfig,
  scheme: PrimaryRestrictionScheme,
  tableConfig: ReadConf,
) extends PartitionReader[InternalRow]
    with Logging {

  private val storage          = StorageHelper(config)
  private val meta             = storage.metaData(tableConfig).get
  private val recordDescriptor = meta.recordType(tableConfig.recordType).getDescriptor
  val count                    = new AtomicLong(0)

  // We always want to emit default values
  val sql = new ProtobufDeserializer(recordDescriptor, partition.schema, emitDefaultValues = true)

  val result: ZStream[Any, Throwable, FdbRecordStore.Continuable[FDBStoredRecord[Message]]] =
    storage.db.streamAsyncContinuation { continue =>
      ZStream.unwrap {
        for {
          rc         <- ZIO.service[FdbRecordContext]
          tableStore <- rc.createOrOpen(meta)
        } yield tableStore
          .scanRecords(
            scheme.range.toRange,
            continue,
            new ScanProperties(
              ExecuteProperties
                .newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setDefaultCursorStreamingMode(CursorStreamingMode.LARGE)
                .build()
            ),
          )
          .mapChunksZIO { ch =>
            count.updateAndGet(x => x + ch.size)
            ZIO.succeed(ch)
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
      throw value
    case Right(value) =>
      sql.deserialize(value.record.getRecord).orNull
  }

  override def close(): Unit = {
    logInfo(s"${getClass.getSimpleName} - closing, ${scheme.range}, (${count.get()} elements)")
    storage.shutdown()
  }

}

class FdbScanPrimaryKeyPartitionReaderCount(
  partition: FdbInputPartition,
  config: SparkFdbConfig,
  scheme: PrimaryRestrictionScheme,
  tableConfig: ReadConf,
) extends PartitionReader[InternalRow]
    with Logging {

  private val storage = StorageHelper(config)
  private val meta    = storage.metaData(tableConfig).get
  val count           = new AtomicLong(0)

  val result: ZStream[Any, Throwable, InternalRow] =
    storage.db.streamAsyncContinuation { continue =>
      ZStream.unwrap {
        for {
          rc         <- ZIO.service[FdbRecordContext]
          tableStore <- rc.createOrOpen(meta)
        } yield tableStore
          .scanRecords(
            scheme.range.toRange,
            continue,
            new ScanProperties(
              ExecuteProperties
                .newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setDefaultCursorStreamingMode(CursorStreamingMode.LARGE)
                .build()
            ),
          )
          .mapChunksZIO { ch =>
            count.updateAndGet(x => x + ch.size)
            ZIO.succeed(ch.map(_ => InternalRow.empty))
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
      throw value
    case Right(value) => value
  }

  override def close(): Unit = {
    logInfo(s"${getClass.getSimpleName} - closing, ${scheme.range}, (${count.get()} elements)")
    storage.shutdown()
  }

}
