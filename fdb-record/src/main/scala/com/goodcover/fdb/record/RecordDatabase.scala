package com.goodcover.fdb.record

import com.apple.foundationdb.record.*
import com.apple.foundationdb.record.metadata.{ Index, IndexAggregateFunction, Key, RecordType }
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath
import com.apple.foundationdb.record.provider.foundationdb.storestate.{
  FDBRecordStoreStateCache,
  MetaDataVersionStampStoreStateCacheFactory
}
import com.apple.foundationdb.record.provider.foundationdb.{
  APIVersion,
  FDBDatabase as RFDBDatabase,
  FDBDatabaseFactoryImpl,
  FDBIndexedRecord,
  FDBLocalityProvider,
  FDBMetaDataStore,
  FDBQueriedRecord,
  FDBRecordContext,
  FDBRecordStore,
  FDBStoreTimer,
  FDBStoredRecord,
  IndexOrphanBehavior,
  IndexScanBounds,
  OnlineIndexer
}
import com.apple.foundationdb.record.query.{ ParameterRelationshipGraph, RecordQuery }
import com.apple.foundationdb.record.query.expressions.QueryComponent
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{ ByteArrayUtil2, Tuple }
import com.apple.foundationdb.*
import com.apple.foundationdb.record.metadata.expressions.KeyExpression
import com.goodcover.fdb.FdbDatabase.Transactor
import com.goodcover.fdb.record.RecordDatabase.FdbRecordStore.{ Continuable, ContinuableError }
import com.goodcover.fdb.{ FdbDatabase, FdbPool }
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.{ Descriptors, Message }
import zio.ZIO.LogAnnotate
import zio.*
import zio.stream.ZStream

import java.lang
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters.*
import scala.jdk.FunctionConverters.*

object RecordDatabase {

  case class FdbMetadata(metadata: RecordMetaData, tablePath: KeySpacePath) {
    def getIndex(name: String): Index = metadata.getIndex(name)

    def withMetadata[A](fn: RecordMetaData => A): A = fn(metadata)

    def commonPrimaryKey: KeyExpression = metadata.commonPrimaryKey()

    def getAllIndexes: List[Index] = metadata.getAllIndexes.asScala.toList

    def recordType(name: String): RecordType = metadata.getRecordType(name)

    def isSplitLongRecords: Boolean = metadata.isSplitLongRecords

    def toProto: RecordMetaDataProto.MetaData = metadata.toProto

    def toProto(excludedDependencies: Array[Descriptors.FileDescriptor]): RecordMetaDataProto.MetaData =
      metadata.toProto(excludedDependencies)

    def getRecordsDescriptor: Descriptors.FileDescriptor = metadata.getRecordsDescriptor

    def getVersion: Int = metadata.getVersion

    override def toString: String = s"FdbMetadata(metadata=$metadata, tablePath=$tablePath, version=$getVersion)"

  }

  class FdbRecordDatabaseFactory(runtime: Runtime[Any], _pool: FdbPool, id: Ref[Long]) extends FDBDatabaseFactoryImpl {
    private implicit val unsafe: Unsafe = Unsafe.unsafe(u => u)
    private val scope: Scope.Closeable  = runtime.unsafe.run(Scope.make).getOrThrow()

    setStoreStateCacheFactory(MetaDataVersionStampStoreStateCacheFactory.newInstance())
    setAPIVersion(APIVersion.fromVersionNumber(_pool.config.apiVersion))

    override def initFDB(): FDB = runtime.unsafe.run {
      ZIO.attemptBlocking(_pool.fdb)
    }
      .getOrThrow()

    override def open(cf: String): Database =
      runtime.unsafe.run {
        ZIO.blocking {
          val newConfig = _pool.config.copy(clusterFile = Option(cf))
          for {
            env <- FdbPool.make().build(scope).provide(ZLayer.succeed(newConfig))
            _   <- ZIO.logInfo(s"Opened a clusterfile ${newConfig.clusterFile}, via factory methods")
            db  <- env.get[FdbPool].transact.build(scope)
          } yield db.get[FdbDatabase].db
        }
      }
        .getOrThrow()

    override def shutdown(): Unit = this.synchronized {
      val localDatabases = databases.values.asScala.toVector
      for (database <- localDatabases)
        database.close()

      runtime.unsafe.run {
        ZIO.blocking {

          ZIO.logDebug(s"Closed the following databases ${localDatabases.mkString("'", "', '", "'")}") *>
            scope.close(Exit.succeed(()))
        }
      }
        .getOrThrow()
    }

    override def getDatabase(cf1: String): RFDBDatabase =
      super.getDatabase(cf1)

    def db(implicit trace: Trace): Task[FdbRecordDatabase] = for {
      _                <- id.update(_ + 1)
      openTransactions <- Ref.make(Set.empty[Long])
      blocking         <- ZIO.attemptBlocking(getDatabase(_pool.config.clusterFile.orNull))
    } yield new FdbRecordDatabase(blocking, id, openTransactions)

    def pool: FdbPool = _pool
  }

  object FdbRecordDatabaseFactory {

    val live: ZLayer[FdbPool, Nothing, FdbRecordDatabaseFactory] = ZLayer.scoped {
      for {
        pool    <- ZIO.service[FdbPool]
        rt      <- ZIO.runtime[Any]
        id      <- Ref.make(0L)
        factory <- ZIO.succeed(new FdbRecordDatabaseFactory(rt, pool, id))
        _       <- ZIO.addFinalizer(ZIO.attemptBlocking(factory.shutdown()).orDie)
      } yield factory
    }
  }

  class FdbRecordDatabase(db: RFDBDatabase, private[record] val idRef: Ref[Long], openTransactions: Ref[Set[Long]]) {

    /**
     * We do a lot of stuff across boundaries, but take care to make it not
     * terrible
     */
    private implicit val unsafe: Unsafe = Unsafe.unsafe(unsafe => unsafe)

    def close: Task[Unit] = ZIO.attemptBlocking(db.close())

    private[fdb] def recordOpenTransaction(id: Long) = openTransactions.update(_ + id)

    private[fdb] def recordClosedTransaction(id: Long) = openTransactions.update(_ - id)

    def database: FdbDatabase =
      new FdbDatabase(db.database(), idRef, openTransactions)

    def storeStateCache: FDBRecordStoreStateCache = db.getStoreStateCache

    def getLocalityProvider: FDBLocalityProvider = db.getLocalityProvider

    def streamAsync[R, A](
      fn: => ZStream[FdbRecordContext & R, Throwable, A]
    )(implicit trace: Trace): ZStream[R, Throwable, A] =
      ZStream.unwrapScoped[R] {
        for {

          completionLatch <- Promise.make[Throwable, Unit]
          txn             <- Promise.make[Throwable, FdbRecordContext]
          _               <- runAsync[Any, Unit](ZIO.serviceWithZIO[FdbRecordContext] { fdb =>
                               txn.succeed(fdb) *> completionLatch.await
                             }).forkScoped
          txFilled        <- txn.await
          _               <- ZIO.addFinalizerExit { e =>
                               completionLatch.done(
                                 e.mapErrorExit {
                                   case e: Throwable => e
                                   case e            => new IllegalStateException(s"Failed for an unknown reason, $e")
                                 }.mapExit(_ => ())
                               )
                             }
        } yield fn.provideSomeEnvironment[R](_.add[FdbRecordContext](txFilled))

      }

    def streamAsyncContinuation[R, A](
      fn: Array[Byte] => ZStream[FdbRecordContext & R, Throwable, A]
    )(implicit trace: Trace): ZStream[R, Throwable, A] = {

      def inner(continuation: Array[Byte]): ZStream[R, Throwable, A] =
        ZStream
          .unwrapScoped[R]
          .apply {
            for {

              completionLatch <- Promise.make[Throwable, Unit]
              txn             <- Promise.make[Throwable, FdbRecordContext]
              _               <- runAsync[R, Unit](ZIO.serviceWithZIO[FdbRecordContext] { fdb =>
                                   txn.succeed(fdb) *> completionLatch.await
                                 }).forkScoped
              txFilled        <- txn.await
              _               <- ZIO.addFinalizerExit { e =>
                                   completionLatch.done(
                                     e.mapErrorExit {
                                       case e: Throwable => e
                                       case e            => new IllegalStateException(s"Failed for an unknown reason, $e")
                                     }.mapExit(_ => ())
                                   )
                                 }
            } yield fn(continuation).provideSomeEnvironment[R](_.add[FdbRecordContext](txFilled))

          }
          .catchSome { case e: ContinuableError =>
            inner(e.continuation.toBytes)
          }

      inner(null)
    }

    def runAsync[R, A](
      fn: FdbRecordContext => ZIO[FdbRecordContext & R, Throwable, A]
    )(implicit trace: Trace): ZIO[R, Throwable, A] =
      runAsync[R, A](ZIO.serviceWithZIO[FdbRecordContext](fn))

    def runAsync[R, A](fn: => ZIO[FdbRecordContext & R, Throwable, A])(implicit trace: Trace): ZIO[R, Throwable, A] =
      ZIO.scoped[R] {
        for {
          runtime <- ZIO.runtime[R]
          id      <- idRef.updateAndGet(_ + 1)
          _       <- recordOpenTransaction(id) *> ZIO.addFinalizer(recordClosedTransaction(id))

          functionToRun = { (txn: FDBRecordContext) =>
                            val cf   = new CompletableFuture[A]()
                            val fork = runtime.unsafe.fork {
                              FdbRecordDatabase.logTxnId(id) {
                                fn.provideSomeEnvironment[R](_.add[FdbRecordContext](new FdbRecordContext(txn, this)))
                              }
                            }

                            fork.unsafe.addObserver {
                              case Exit.Success(value)                    => cf.complete(value): Unit
                              case Exit.Failure(c) if c.isInterruptedOnly => cf.cancel(true): Unit
                              case Exit.Failure(c)                        => cf.completeExceptionally(c.squash): Unit
                            }

                            cf.asInstanceOf[CompletableFuture[? <: A]]

                          }.asJava
          result       <- ZIO.fromCompletableFuture[A](db.runAsync[A](functionToRun))
        } yield result
      }

    def loadMetadata(metaPath: KeySpacePath, maybeDescriptor: Option[FileDescriptor], keyspacePath: KeySpacePath)(implicit
      trace: Trace
    ): ZIO[FdbRecordContext, Throwable, Option[FdbMetadata]] =
      for {
        ctx    <- ZIO.service[FdbRecordContext]
        ds     <- ZIO.succeed(new FDBMetaDataStore(ctx.ctx, metaPath))
        _      <- ZIO.succeed(maybeDescriptor.foreach(fn => ds.setLocalFileDescriptor(fn)))
        meta   <- ZIO.fromCompletableFuture(ds.getRecordMetaDataAsync(false))
        result <-
          if (meta == null) {
            ZIO.succeed(Option.empty[FdbMetadata])
          } else {
            val result = FdbMetadata(meta, keyspacePath)
            ZIO.logDebug(s"Found metadata, $result").as(Some(result))
          }
      } yield result

    def unsafeAlterMeta[R, A](path: KeySpacePath, rmdb: FdbMetadata)(
      fn: FDBMetaDataStore => RIO[R & FdbRecordContext, A]
    )(implicit trace: Trace): RIO[R, A] =
      runAsync[R, A](for {
        ctx <- ZIO.service[FdbRecordContext]
        _   <- ZIO.logDebug(s"unsafeAlterMeta, saving the metadata to the path=[$path]")
        ds  <- ZIO.succeed(new FDBMetaDataStore(ctx.ctx, path))
        _   <- preloadMetadata(path, rmdb.metadata)
        r   <- fn(ds)
      } yield r)

    def preloadMetadata(metaPath: KeySpacePath, rmdb: RecordMetaData)(implicit
      trace: Trace
    ): ZIO[FdbRecordContext, Throwable, Unit] =
      for {
        ctx <- ZIO.service[FdbRecordContext]
        ds  <- ZIO.succeed(new FDBMetaDataStore(ctx.ctx, metaPath))
        _   <- ZIO.fromCompletableFuture(ds.preloadMetaData(rmdb))
      } yield ()

    def onlineIndexBuilder: OnlineIndexer.Builder = OnlineIndexer.newBuilder().setDatabase(db)
  }

  object FdbRecordDatabase {

    val live: ZLayer[FdbRecordDatabaseFactory, Nothing, FdbRecordDatabase] =
      ZLayer.scoped {
        for {
          pool <- ZIO.service[FdbRecordDatabaseFactory]
          db   <- pool.db.orDie
        } yield db
      }

    private def logTxnId(id: Long): LogAnnotate =
      ZIO.logAnnotate(FdbRecordContext.TxId, id.toString)
  }

  class FdbRecordContext(_ctx: FDBRecordContext, db: FdbRecordDatabase) {

    def ctx: FDBRecordContext = _ctx

    def createOrOpen(meta: FdbMetadata)(implicit trace: Trace): Task[FdbRecordStore] =
      ZIO
        .fromCompletableFuture(
          FDBRecordStore
            .newBuilder()
            .setMetaDataProvider(meta.metadata)
            .setStoreStateCache(db.storeStateCache)
            .setContext(_ctx)
            .setKeySpacePath(meta.tablePath)
            .createOrOpenAsync()
        )
        .map(rs => new FdbRecordStore(rs, meta, db))

    def deleteStore(keySpacePath: KeySpacePath)(implicit unsafe: Unsafe, trace: Trace): Task[Unit] =
      ZIO.attemptBlocking(FDBRecordStore.deleteStore(ctx, keySpacePath))
  }

  object FdbRecordContext {
    private[fdb] val TxId = "txRId"

  }

  class FdbRecordStore(store: FDBRecordStore, meta: FdbMetadata, recordDb: FdbRecordDatabase) {

    /** Expose the underlying transaction in non-record terms */
    def underlyingTransaction(implicit trace: Trace): ZIO[Any, Nothing, Transactor[Transaction]] =
      recordDb.idRef.get.map { id =>
        new Transactor(store.ensureContextActive(), id)
      }

    /** Use sparingly */
    def underlyingStore: FDBRecordStore = store

    /**
     * Could block but likely opened with createOrOpenAsync which preloads this
     * call and [[recordsSubspace()]]
     */
    def storeState(): RecordStoreState = store.getRecordStoreState

    def recordsSubspace(): Subspace = store.recordsSubspace()

    def timer: FDBStoreTimer = store.getTimer

    def metadata: FdbMetadata = meta

    def indexSubspace(index: FdbMetadata => Index): Subspace = store.indexSubspace(index(meta))

    def evaluateAggregateFunction(
      recordTypes: List[String],
      aggregateFunction: IndexAggregateFunction,
      evaluated: Key.Evaluated,
      isolationLevel: IsolationLevel
    )(implicit trace: Trace): Task[Tuple] =
      ZIO.fromCompletableFuture(
        store.evaluateAggregateFunction(recordTypes.asJava, aggregateFunction, evaluated, isolationLevel)
      )

    def estimateRecordsSize(implicit trace: Trace): Task[Long] =
      ZIO.fromCompletableFuture(store.estimateRecordsSizeAsync()).map(_.toLong)

    def estimateStoreSize(implicit trace: Trace): Task[Long] =
      ZIO.fromCompletableFuture(store.estimateStoreSizeAsync()).map(_.toLong)

    def approximateTransactionSize(implicit trace: Trace): Task[Long] =
      ZIO.fromCompletableFuture(store.getContext.getApproximateTransactionSize).map(_.toLong)

    def saveRecord(m: Message)(implicit trace: Trace): Task[FDBStoredRecord[Message]] =
      ZIO.fromCompletableFuture(store.saveRecordAsync(m))

    def primaryKeyBoundaries(low: Tuple, high: Tuple)(implicit trace: Trace): ZStream[Any, Throwable, Tuple] =
      ZStream.unwrapScoped {
        for {
          cursor <- ZIO.succeed(store.getPrimaryKeyBoundaries(low, high))
          _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).orDie)
        } yield FdbRecordStore.cursorToZStream(cursor).map(_.record)
      }

    def primaryKeyBoundaries(range: Option[TupleRange])(implicit trace: Trace): ZStream[Any, Throwable, Tuple] =
      ZStream.unwrapScoped {
        for {
          cursor <- ZIO.succeed(store.getPrimaryKeyBoundaries(range.orNull))
          _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).orDie)
        } yield FdbRecordStore.cursorToZStream(cursor).map(_.record)
      }

    def indexBoundaries(index: FdbMetadata => Index, range: Option[TupleRange])(implicit
      trace: Trace
    ): ZStream[Any, Throwable, Tuple] =
      ZStream.unwrapScoped {
        for {
          beginAndEnd <- ZIO.succeed {
                           val theRange = range.getOrElse(TupleRange.ALL)
                           theRange.toRange(indexSubspace(index))
                         }
        } yield indexBoundaries(index, beginAndEnd.begin, beginAndEnd.end)
      }

    private def indexBoundaries(index: FdbMetadata => Index, rangeStart: Array[Byte], rangeEnd: Array[Byte])(implicit
      trace: Trace
    ): ZStream[Any, Throwable, Tuple] = {
      val transaction          = store.ensureContextActive()
      val cursor               = recordDb.getLocalityProvider.getBoundaryKeys(transaction, rangeStart, rangeEnd)
      val hasSplitRecordSuffix = metadata.isSplitLongRecords
      val closure              = new DistinctFilterCursorClosure

      FdbRecordStore
        .cursorToZStream(
          RecordCursor
            .flatMapPipelined(
              (_: Array[Byte]) => RecordCursor.fromIterator(store.getExecutor, cursor),
              (result: Array[Byte], _: Array[Byte]) =>
                RecordCursor
                  .fromIterator(store.getExecutor, transaction.snapshot.getRange(result, rangeEnd, 1).iterator),
              null,
              FDBRecordStore.DEFAULT_PIPELINE_SIZE
            )
            .map { (keyValue: KeyValue) =>
              val recordKey = indexSubspace(index).unpack(keyValue.getKey)
              if (hasSplitRecordSuffix) recordKey.popBack
              else recordKey

            }
            .filter(closure.pred)
        )
        .map(_.record)
        .ensuring(ZIO.attempt(cursor.close()).orDie)
    }

    def deleteAllRecords(implicit unsafe: Unsafe, trace: Trace): Task[Unit] =
      ZIO.attemptBlocking(store.deleteAllRecords())

    def loadRecord(t: Tuple)(implicit trace: Trace): Task[FDBStoredRecord[Message]] =
      ZIO.fromCompletableFuture(store.loadRecordAsync(t))

    def queryPlan(query: RecordQuery): RecordQueryPlan =
      store.planQuery(query)

    def queryPlan(query: RecordQuery, parameterRelationshipGraph: ParameterRelationshipGraph): RecordQueryPlan =
      store.planQuery(query, parameterRelationshipGraph)

    def executeQuery(
      query: RecordQuery
    )(implicit trace: Trace): ZStream[Any, Throwable, Continuable[FDBQueriedRecord[Message]]] =
      executeQuery(query, null)

    def executeQuery(query: RecordQuery, continuation: Array[Byte])(implicit
      trace: Trace
    ): ZStream[Any, Throwable, Continuable[FDBQueriedRecord[Message]]] =
      executeQuery(
        query,
        continuation,
        ExecuteProperties.newBuilder().setIsolationLevel(IsolationLevel.SNAPSHOT).build()
      )

    /**
     * Run a primary key scan over the entire keyspace
     */
    def scanRecords(range: TupleRange, continuation: Array[Byte], scanProperties: ScanProperties)(implicit
      trace: Trace
    ): ZStream[Any, Throwable, Continuable[FDBStoredRecord[Message]]] =
      ZStream.unwrapScoped {
        for {
          cursor <- ZIO.succeed(store.scanRecords(range, continuation, scanProperties))
          _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).ignore)
        } yield FdbRecordStore.cursorToZStream(cursor)
      }

    /**
     * Scan the records pointed to by an index, using a single
     * scan-and-dereference FDB operation. This method requires that there is a
     * common primary key length to all types defined for the index.
     * @return
     */
    def scanIndexRemoteFetch(
      range: FdbMetadata => Index,
      scanBounds: IndexScanBounds,
      continuation: Array[Byte],
      scanProperties: ScanProperties,
      orphanBehavior: IndexOrphanBehavior
    )(implicit trace: Trace): ZStream[Any, Throwable, Continuable[FDBIndexedRecord[Message]]] =
      ZStream.unwrapScoped {
        for {
          cursor <- ZIO.succeed(
                      store.scanIndexRemoteFetch(range(metadata), scanBounds, continuation, scanProperties, orphanBehavior)
                    )
          _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).ignore)
        } yield FdbRecordStore.cursorToZStream(cursor)
      }

    /**
     * Fetches indexed records using the provided index cursor and orphan
     * behavior configuration.
     *
     * @param indexCursor
     *   a cursor over index entries to fetch the records
     * @param orphanBehavior
     *   defines how orphaned index entries should be handled during the fetch
     * @return
     *   a stream of continuable indexed records wrapped in a ZIO stream
     */
    def fetchIndexRecords(
      indexCursor: RecordCursor[IndexEntry],
      orphanBehavior: IndexOrphanBehavior
    )(implicit trace: Trace): ZStream[Any, Throwable, Continuable[FDBIndexedRecord[Message]]] =
      ZStream.unwrapScoped {
        for {
          cursor <- ZIO.succeed(
                      store.fetchIndexRecords(indexCursor, orphanBehavior)
                    )
          _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).ignore)
        } yield FdbRecordStore.cursorToZStream(cursor)
      }

    /**
     * Scans an index from the underlying record store using the specified
     * parameters.
     *
     * @param indexFn
     *   A function that retrieves the index from the metadata.
     * @param scanBounds
     *   The bounds for the index scan.
     * @param continuation
     *   A continuation token that indicates where to resume the scan.
     * @param scanProperties
     *   Properties defining the behavior of the scan, such as limits and
     *   isolation levels.
     * @return
     *   A resource-safe effect that, when executed, provides a cursor over the
     *   index entries.
     */
    def scanIndex(
      indexFn: FdbMetadata => Index,
      scanBounds: IndexScanBounds,
      continuation: Array[Byte],
      scanProperties: ScanProperties,
    )(implicit trace: Trace): RIO[Scope, RecordCursor[IndexEntry]] =
      for {
        cursor <- ZIO.succeed(
                    store.scanIndex(indexFn(metadata), scanBounds, continuation, scanProperties)
                  )
        _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).ignoreLogged)
      } yield cursor

    def executeQuery(query: RecordQuery, continuation: Array[Byte], executeProperties: ExecuteProperties)(implicit
      trace: Trace
    ): ZStream[Any, Throwable, Continuable[FDBQueriedRecord[Message]]] =
      ZStream.unwrapScoped {
        for {
          plan   <- ZIO.succeed(store.planQuery(query))
          cursor <- ZIO.succeed(store.executeQuery(plan, continuation, executeProperties))
          _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).ignore)
        } yield FdbRecordStore.cursorToZStream(cursor)
      }

    def deleteRecords(tuple: Tuple)(implicit trace: Trace): Task[Unit] =
      ZIO.fromCompletableFuture(store.deleteRecordAsync(tuple)).unit

    /** This does NOT work well */
    def deleteRecordsWhere(recordType: String, query: QueryComponent)(implicit trace: Trace): Task[Unit] =
      ZIO.fromCompletableFuture(store.deleteRecordsWhereAsync(recordType, query)).unit

    /** This does NOT work well */
    def deleteRecordsWhere(query: QueryComponent)(implicit trace: Trace): Task[Unit] =
      ZIO.fromCompletableFuture(store.deleteRecordsWhereAsync(query)).unit

    def executeQuery(
      query: RecordQueryPlan
    )(implicit trace: Trace): ZStream[Any, Throwable, Continuable[FDBQueriedRecord[Message]]] =
      ZStream.unwrapScoped {
        for {
          cursor <- ZIO.succeed(store.executeQuery(query))
          _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).orDie)
        } yield FdbRecordStore.cursorToZStream(cursor)
      }

    def executeQuery(
      query: RecordQueryPlan,
      continuation: Array[Byte],
      executeProperties: ExecuteProperties,
      evaluationContext: EvaluationContext,
    )(implicit trace: Trace): ZStream[Any, Throwable, Continuable[FDBQueriedRecord[Message]]] =
      ZStream.unwrapScoped {
        for {
          cursor <- ZIO.succeed(store.executeQuery(query, continuation, evaluationContext, executeProperties))
          _      <- ZIO.addFinalizer(ZIO.attempt(cursor.close()).orDie)
        } yield FdbRecordStore.cursorToZStream(cursor).map(_.map(qr => qr.getQueriedRecord))
      }

    def snapshotRecordCount(implicit trace: Trace): Task[lang.Long] =
      ZIO.fromCompletionStage(store.getSnapshotRecordCount)

    def snapshotRecordCountForRecordType(recordType: String)(implicit trace: Trace): Task[lang.Long] =
      ZIO.fromCompletionStage(store.getSnapshotRecordCountForRecordType(recordType))
  }

  object FdbRecordStore {

    case class Continuable[+A](continuation: RecordCursorContinuation, record: A) {

      def map[A0](fn: A => A0): Continuable[A0] =
        copy(continuation = continuation, record = fn(record))
    }

    case class ContinuableError(err: FDBException, continuation: RecordCursorContinuation)
        extends Throwable(
          s"Continuable error=[$err], continuation=[${ByteArrayUtil2.loggable(continuation.toBytes)}]",
          null,
          true,
          false
        )

    /**
     * There might be a more parallel way to do this but I can't think of how..
     */
    private def cursorToZStream[R, A](
      cursor: RecordCursor[A]
    )(implicit trace: Trace): ZStream[R, Throwable, Continuable[A]] = {
      var lastContinuation: RecordCursorContinuation = null
      ZStream
        .unfoldZIO(cursor) { cursor =>
          ZIO
            .fromCompletableFuture(cursor.onNext())
            .mapError {
              case fdb: FDBException if fdb.getCode == 1007 && lastContinuation != null =>
                ContinuableError(fdb, lastContinuation)
              case e                                                                    => e
            }
            .flatMap { result =>
              lastContinuation = result.getContinuation
              if (result.hasNext)
                ZIO.some((Continuable(lastContinuation, result.get()), cursor))
              else
                ZIO.succeed(cursor.close()).as(None)
            }

        }
    }

  }

  private class DistinctFilterCursorClosure {
    private var previousKey: Tuple = null

    private[fdb] def pred(key: Tuple) = if (key == previousKey) false
    else {
      previousKey = key
      true
    }
  }
}
