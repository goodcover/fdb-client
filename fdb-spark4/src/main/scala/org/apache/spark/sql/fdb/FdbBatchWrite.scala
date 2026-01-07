package org.apache.spark.sql.fdb

import com.goodcover.fdb.Helpers
import com.goodcover.fdb.record.RecordDatabase.FdbRecordContext
import com.google.protobuf.Message
import nl.vroste.rezilience.RateLimiter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.*
import org.apache.spark.sql.fdb.FdbWriter.CommitMessage
import org.apache.spark.sql.proto.ProtobufSerializer
import org.apache.spark.sql.types.StructType
import zio.*
import zio.stream.ZStream

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

class FdbBatchWrite(schema: StructType, conf: FdbTable.MetadataWith[ReadConf]) extends BatchWrite with Logging {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new FdbBatchWriterFactory(schema, conf.config, conf.dbConfig, info.numPartitions())

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val commit = messages.foldLeft(CommitMessage(0L, 0L, 0L)) {
      case (acc, cm: CommitMessage) =>
        acc + cm
      case (acc, _)                 => acc
    }
    logInfo(
      s"FdbBatchWrite - [$commit], " +
        s"avg per partition = [${Helpers.humanReadableSize(commit.bytes / messages.length, false)}], " +
        s"avg records = [${commit.records / messages.length}]"
    )
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit =
    logInfo("FdbBatchWrite - abort")
}

class FdbBatchWriterFactory(
  schema: StructType,
  tableConfig: ReadConf,
  dbConfig: SparkFdbConfig,
  totalPartitions: Int
) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val localConfig = tableConfig
    if (localConfig.batchedTransactionalInserts.exists(_ > 1))
      new FdbBatchWriter(localConfig, dbConfig, schema, partitionId, taskId, totalPartitions)
    else
      new FdbWriter(localConfig, dbConfig, schema, partitionId, taskId, totalPartitions)

  }
}

class FdbWriter(
  tableConfig: ReadConf,
  dbConfig: SparkFdbConfig,
  schema: StructType,
  partitionId: Int,
  taskId: Long,
  totalPartitions: Int
) extends DataWriter[InternalRow]
    with Logging {
  private val storage                = StorageHelper(dbConfig)
  private val meta                   = storage.metaData(tableConfig).get
  private val recordDescriptor       = meta.recordType(tableConfig.recordType).getDescriptor
  private val sql                    = new ProtobufSerializer(schema, recordDescriptor, true)
  private val scope: Scope.Closeable = storage.unsafeRunBlocking(Scope.make)
  private val rateLimiter            = tableConfig.maxRequestsPerSecond.fold(FdbWriter.noOp) { limit =>
    val minLimit = Math.max(limit / totalPartitions, 1)
    storage.unsafeRunBlocking(RateLimiter.make(minLimit, 1.second).provideSomeEnvironment(_.add(scope)))
  }

  private val approximateBytes = new AtomicLong()
  private val records          = new AtomicLong()

  override def write(record: InternalRow): Unit = {
    val result = sql.serialize(record).asInstanceOf[Message]
    storage.unsafeRunBlocking {
      rateLimiter {
        storage.db.runAsync[Any, Unit] { (ctx: FdbRecordContext) =>
          for {
            store <- ctx.createOrOpen(meta)
            _     <- store.saveRecord(result)
            _     <- ZIO.succeed(records.updateAndGet(_ + 1L)) &> store.approximateTransactionSize.map(bytes =>
                       approximateBytes.updateAndGet(_ + bytes)
                     )
          } yield ()
        }
      }

    }
  }

  override def commit(): WriterCommitMessage =
    CommitMessage(partitionId.toLong, approximateBytes.getAndSet(0L), records.getAndSet(0))

  override def abort(): Unit =
    storage.unsafeRunBlocking {
      logInfo(s"FdbWriter($partitionId, $taskId) - abort")
      scope.close(Exit.fail(()))
    }

  override def close(): Unit =
    storage.unsafeRunBlocking {
      logInfo(s"FdbWriter($partitionId, $taskId) - close, wrote ${records.get()}")
      scope.close(Exit.succeed(()))
    }

}

object FdbWriter {
  private[fdb] val noOp: RateLimiter = new RateLimiter {
    override def apply[R, E, A](task: ZIO[R, E, A]): ZIO[R, E, A] = task
  }

  private[fdb] case class CommitMessage(partitionId: Long, bytes: Long, records: Long) extends WriterCommitMessage {
    override def toString: String = s"Commit(bytes=${Helpers.humanReadableSize(bytes, false)}, records=$records)"

    def +(other: CommitMessage): CommitMessage =
      CommitMessage(partitionId = partitionId, bytes = bytes + other.bytes, records = records + other.records)
  }

}

class FdbBatchWriter(
  tableConfig: ReadConf,
  dbConfig: SparkFdbConfig,
  schema: StructType,
  partitionId: Int,
  taskId: Long,
  totalPartitions: Int
) extends DataWriter[InternalRow]
    with Logging {
  private val storage          = StorageHelper(dbConfig)
  private val meta             = storage.metaData(tableConfig).get
  private val recordDescriptor = meta.recordType(tableConfig.recordType).getDescriptor
  private val sql              = new ProtobufSerializer(schema, recordDescriptor, true)
  private val rateLimiter      = storage.makeRateLimiter(tableConfig, totalPartitions)

  private val approximateBytes = new AtomicLong()
  private val records          = new AtomicLong()
  private val promises         = mutable.Set[Promise[Throwable, Unit]]()

  private val configBatch    = tableConfig.batchedTransactionalInserts.getOrElse(1)
  private val configParallel = tableConfig.parallelBatchWrites.getOrElse(1)

  private lazy val queue: Queue[(Promise[Throwable, Unit], Message)] = storage.unsafeRunBlocking((for {
    q <- Queue.bounded[(Promise[Throwable, Unit], Message)](1024)
    _ <- ZStream
           .fromQueue(q, maxChunkSize = configBatch)
           .mapChunks(chunks => Chunk.single(chunks))
           .mapZIOParUnordered(configParallel) { messages =>
             storage.db
               .runAsync[Any, Chunk[(Promise[Throwable, Unit], Message)]] { (ctx: FdbRecordContext) =>
                 for {
                   store <- ctx.createOrOpen(meta)
                   _     <- ZIO.foreachDiscard(messages.map(_._2))(store.saveRecord)
                   _     <- store.approximateTransactionSize.map { bytes =>
                              records.updateAndGet(_ + 1L)
                              approximateBytes.updateAndGet(_ + bytes)
                            }
                 } yield messages
               }
               .onExit {
                 case Exit.Success(_)     =>
                   ZIO.foreach(messages.map(_._1).tapEach(promises.remove))(_.succeed(()))
                 case Exit.Failure(cause) =>
                   val result = ZIO.logErrorCause("Exception while inserting", cause) *>
                     ZIO.foreach(messages.map(_._1))(_.refailCause(cause))
                   result
               }
           }
           .runDrain
           .forkScoped
  } yield q).provide(ZLayer.succeed(storage.scope)))

  override def write(record: InternalRow): Unit = {
    val result = sql.serialize(record).asInstanceOf[Message]
    val _      = storage.unsafeRun {
      rateLimiter {
        Promise.make[Throwable, Unit].flatMap { promise =>
          queue.offer((promise, result)).as(promises.add(promise))
        }
      }

    }
  }

  override def commit(): WriterCommitMessage = {
    val _ = storage.unsafeRun(ZIO.foreachPar(promises.toSeq) { promise =>
      promise.await
    })
    CommitMessage(partitionId.toLong, approximateBytes.get(), records.get())
  }

  override def abort(): Unit = {
    logError(s"FdbBatchWriter($partitionId, $taskId) - abort")
    storage.shutdown()
  }

  override def close(): Unit = {
    logWarning(
      s"FdbBatchWriter($partitionId, $taskId) - close, wrote ${records.getAndSet(0L)}, approximately ${Helpers.humanReadableSize(approximateBytes.getAndSet(0L), false)}"
    )
    storage.shutdown()
  }

}
