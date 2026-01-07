package org.apache.spark.sql.fdb

import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord
import com.apple.foundationdb.record.query.RecordQuery
import com.apple.foundationdb.record.query.expressions.{ Query, QueryComponent }
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan
import com.apple.foundationdb.record.{ CursorStreamingMode, EvaluationContext, ExecuteProperties, IsolationLevel }
import com.goodcover.fdb.record.RecordDatabase.{ FdbRecordContext, FdbRecordStore }
import com.google.protobuf.Message
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.proto.ProtobufDeserializer
import zio.stream.ZStream
import zio.{ ZIO, ZLayer }

import scala.jdk.CollectionConverters.*

/** Most of the heavy lifting is here */
class FdbScanNaivePartitionReader(
  partition: FdbInputPartition,
  config: SparkFdbConfig,
  optionalAdditionalQuery: Option[QueryComponent],
  tableConfig: ReadConf,
) extends PartitionReader[InternalRow] {

  private val storage          = StorageHelper(config)
  private val meta             = storage.metaData(tableConfig).get
  private val queryBuilder     = partition.filters
  private val recordDescriptor = meta.recordType(tableConfig.recordType).getDescriptor

  // We always want to emit default values
  val sql = new ProtobufDeserializer(recordDescriptor, partition.schema, emitDefaultValues = true)

  private val filters  = queryBuilder.keys.flatMap(FdbScanBuilder.compileFilter(_, recordDescriptor))
  private val toFilter =
    (if (filters.length <= 1)
       filters.headOption
     else
       Some(Query.and(filters.asJava))) match {
      case Some(result) =>
        optionalAdditionalQuery match {
          case Some(value) => Some(Query.and(result, value))
          case None        => Some(result)
        }
      case None         => optionalAdditionalQuery
    }

  private val plan: RecordQueryPlan = storage.unsafeRun {
    for {
      (timing, plan) <- storage.db.runAsync { rc =>
                          rc.createOrOpen(meta).flatMap { store =>
                            val rq = RecordQuery.newBuilder().setRecordType(tableConfig.recordType)
                            toFilter.foreach(filter => rq.setFilter(filter))
                            ZIO.attempt(store.queryPlan(rq.build()))
                          }
                        }.timed
      _              <- ZIO.logInfo(s"Took ${timing.toMillis}ms to create the plan, $plan")
    } yield plan

  }

  val result: ZStream[Any, Throwable, FdbRecordStore.Continuable[FDBQueriedRecord[Message]]] =
    storage.db.streamAsyncContinuation { continue =>
      ZStream.unwrap {
        for {
          rc         <- ZIO.service[FdbRecordContext]
          tableStore <- rc.createOrOpen(meta)
        } yield tableStore
          .executeQuery(
            plan,
            continue,
            ExecuteProperties
              .newBuilder()
              .setIsolationLevel(IsolationLevel.SNAPSHOT)
              .setDefaultCursorStreamingMode(CursorStreamingMode.LARGE)
              .build(),
            EvaluationContext.EMPTY
          )
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

  override def close(): Unit =
    storage.shutdown()

}

/** Use the index directly. */
class FdbScanCountPartitionReader(config: SparkFdbConfig, tableConfig: ReadConf) extends PartitionReader[InternalRow] {

  private val storage = StorageHelper(config)
  private val meta    = storage.metaData(tableConfig).get

  private val query = storage.db.runAsync { rc =>
    for {
      store <- rc.createOrOpen(meta)
      count <- store.snapshotRecordCountForRecordType(tableConfig.recordType)
    } yield count
  }

  private val result = storage.unsafeRunBlocking {
    query.map(result => Iterator.fill(result.toInt)(InternalRow.empty))
  }

  override def next(): Boolean = result.hasNext

  override def get(): InternalRow = result.next()

  override def close(): Unit =
    storage.shutdown()
}
