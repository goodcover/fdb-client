package com.goodcover.fdb.record.es

import com.apple.foundationdb.record.*
import com.apple.foundationdb.record.metadata.{ IndexAggregateFunction, Key }
import com.apple.foundationdb.record.provider.foundationdb.*
import com.apple.foundationdb.record.provider.foundationdb.keyspace.{ KeySpace, KeySpaceDirectory, KeySpacePath }
import com.apple.foundationdb.record.query.RecordQuery
import com.apple.foundationdb.record.query.expressions.Query
import com.apple.foundationdb.tuple.{ ByteArrayUtil, Tuple }
import com.goodcover.fdb.record.{ RecordConfig, RecordKeySpace }
import com.goodcover.fdb.record.RecordDatabase.*
import com.goodcover.fdb.record.es.EventsourceLayer.{ SnapshotSelectionCriteria, TagOffset }
import com.goodcover.fdb.record.es.proto.*
import com.google.protobuf.Message
import zio.stream.ZStream
import zio.{ LogAnnotation, RIO, Ref, Task, Trace, UIO, Unsafe, ZEnvironment, ZIO, ZLayer }

import scala.annotation.unused
import scala.jdk.CollectionConverters.*

/**
 * The main "untyped" class for dealing with Eventsourcing in FoundationDB.
 * Under the covers this uses the record layer, which is exceptionally well
 * designed, to perform the basic operations of eventsourcing.
 *
 *   - replay
 *   - append
 *   - tailing, watching for changes in keys
 *   - snapshots
 */
class EventsourceLayer(recordDb: FdbRecordDatabase, cfg: RecordConfig, metaRef: Ref[Option[FdbMetadata]]) {

  private def getMetaData(implicit trace: Trace): UIO[FdbMetadata] =
    for {
      result <-
        metaRef.get.some
          .orElse(loadMetadata.some)
          .orElseFail(cfg.localMetadata)
          .merge
    } yield result

  private def mkStore(rc: FdbRecordContext)(implicit trace: Trace): ZIO[Any, Throwable, FdbRecordStore] =
    getMetaData.flatMap { meta =>
      rc.createOrOpen(meta)
    }

  private def convPersistentRepr(rec: Message): PersistentRepr =
    PersistentRepr.fromJavaProto(Fdbrecord.PersistentRepr.newBuilder().mergeFrom(rec).build())

  private def convPersistentRepr(rec: FDBQueriedRecord[Message]): PersistentRepr =
    convPersistentRepr(rec.getRecord)

  private def convPersistentRepr(rec: FDBIndexedRecord[Message]): PersistentRepr =
    convPersistentRepr(rec.getRecord)

  private def convSnapshot(rec: FDBQueriedRecord[Message]): Snapshot =
    Snapshot.fromJavaProto(Fdbrecord.Snapshot.newBuilder().mergeFrom(rec.getRecord).build())

  def loadMetadata(implicit trace: Trace): ZIO[Any, Throwable, Option[FdbMetadata]] =
    recordDb
      .runAsync[Any, Option[FdbMetadata]](
        recordDb
          .loadMetadata( //
            metaPath = cfg.metaPath,
            maybeDescriptor = Some(cfg.localFileDescriptor),
            keyspacePath = cfg.tablePath
          )
      )
      .flatMap { maybeSome =>
        metaRef.updateAndGet(_ => maybeSome) <*
          ZIO.when(cfg.persistLocalMetadata) {
            saveMetadata
          }
      }

  def unsafeAlterMeta[R, A](fn: FDBMetaDataStore => RIO[R & FdbRecordContext, A])(implicit trace: Trace): RIO[R, A] =
    getMetaData.flatMap { meta =>
      recordDb.unsafeAlterMeta[R, A](cfg.metaPath, meta)(fn)
    }

  def saveMetadata(implicit trace: Trace): Task[Unit] =
    recordDb.runAsync {
      for {
        ctx      <- ZIO.service[FdbRecordContext]
        metaPath <- ZIO.succeed(cfg.metaPath)
        ds       <- ZIO.succeed(new FDBMetaDataStore(ctx.ctx, metaPath))
        loaded   <- recordDb.loadMetadata(metaPath, Some(cfg.localFileDescriptor), cfg.tablePath)
        _        <- if (loaded.isEmpty)
                      ZIO.logDebug(s"saving metadata metaData to the path=[$metaPath], version=[${cfg.localMetadata.getVersion}]") *>
                        ZIO.fromCompletableFuture(ds.saveAndSetCurrent(cfg.localMetadata.toProto))
                    else {
                      if (loaded.get.getVersion != cfg.localMetadata.getVersion)
                        ZIO.logDebug(
                          s"updating metadata metaData to the path=[$metaPath], localVersion=[${cfg.localMetadata.getVersion}], remoteVersion=[${loaded.get.getVersion}]"
                        ) *>
                          ZIO.fromCompletableFuture(ds.updateRecordsAsync(cfg.localFileDescriptor))
                      else ZIO.unit
                    }
      } yield ()
    }

  def rebuildDefaultIndexes(implicit trace: Trace): Task[Seq[String]] = recordDb.runAsync(_ => rebuildIndexes())

  def rebuildIndexes()(implicit trace: Trace): Task[Seq[String]] = recordDb.runAsync { ctx =>
    for {
      meta   <- getMetaData
      store  <- ctx.createOrOpen(meta)
      result <- ZIO.foreachPar(store.storeState().getDisabledIndexNames.asScala.toSeq) { indexName =>
                  val builder = recordDb.onlineIndexBuilder.setRecordStore(store.underlyingStore).setIndex(indexName).build()

                  ZIO.fromCompletableFuture(builder.buildIndexAsync()).as(indexName)
                }

    } yield result

  }

  /**
   * Executes a transaction with a provided FdbRecordStore.
   *
   * This could potentially replace some boilerplate locally but it is to expose
   * more functionality than what we currently provide
   *
   * @param fn
   *   Function that takes an FdbRecordStore and returns a ZIO effect that
   *   requires FdbRecordContext which is provided by the layer, so optional to
   *   use
   * @return
   *   ZIO effect that executes the transaction and returns the result
   */
  def withStoreTransaction[A](
    fn: FdbRecordStore => ZIO[FdbRecordContext, Throwable, A]
  )(implicit trace: Trace): ZIO[Any, Throwable, A] = recordDb.runAsync {
    for {
      rc     <- ZIO.service[FdbRecordContext]
      store  <- mkStore(rc)
      result <- fn(store).provideEnvironment(ZEnvironment(rc))
    } yield result
  }

  /**
   * Streams persisted events (PersistentRepr) for a specific persistenceId.,
   * from and to are both inclusive
   */
  def currentEventsById(
    persistenceId: String,
    fromSequenceNr: Long = 0L,
    toSequenceNr: Long = Long.MaxValue,
    max: Int = Int.MaxValue
  )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] =
    recordDb.streamAsyncContinuation { continue =>
      ZStream.unwrap {

        for {
          rc      <- ZIO.service[FdbRecordContext]
          evStore <- mkStore(rc)
          query   <- ZIO.succeed {
                       RecordQuery
                         .newBuilder()
                         .setRecordType("PersistentRepr")
                         .setFilter(buildEventsRangeQuery(persistenceId, fromSequenceNr, toSequenceNr))
                         .build()

                     }
        } yield evStore
          .executeQuery(query, continue, ExecuteProperties.newBuilder().setReturnedRowLimit(max).build())
          .map(continue => convPersistentRepr(continue.record))
      }

    }

  /**
   * Deletes events for a specific persistenceId within a sequence number range.
   *
   * @param persistenceId
   *   identifier of the persistent actor
   * @param fromSequenceNr
   *   start of sequence number range (inclusive)
   * @param toSequenceNr
   *   end of sequence number range (inclusive)
   * @return
   *   Task that completes when all matching events are deleted
   */
  def deleteFromTo(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long)(implicit trace: Trace): Task[Unit] =
    recordDb.runAsync {
      for {
        rc      <- ZIO.service[FdbRecordContext]
        evStore <- mkStore(rc)
        _       <- evStore
                     .executeQuery(
                       RecordQuery
                         .newBuilder()
                         .setRecordType("PersistentRepr")
                         .setFilter(buildEventsRangeQuery(persistenceId, fromSequenceNr, toSequenceNr))
                         .build()
                     )
                     .mapZIO { results =>
                       evStore.deleteRecords(results.record.getStoredRecord.getPrimaryKey)
                     }
                     .runCollect
      } yield ()
    }

  def deleteTo(persistenceId: String, toSequenceNr: Long)(implicit trace: Trace): Task[Unit] = recordDb.runAsync {
    deleteFromTo(persistenceId = persistenceId, fromSequenceNr = 0L, toSequenceNr = toSequenceNr)
  }

  def highestSequenceNr(persistenceId: String, @unused fromSequenceNr: Long)(implicit trace: Trace): Task[Long] =
    recordDb.runAsync {
      for {
        rc     <- ZIO.service[FdbRecordContext]
        store  <- mkStore(rc)
        index   = store.metadata.getIndex("maxSeqNr")
        fn      = new IndexAggregateFunction(FunctionNames.MAX_EVER, index.getRootExpression, index.getName)
        result <- store
                    .evaluateAggregateFunction(
                      "PersistentRepr" :: Nil,
                      fn,
                      Key.Evaluated.scalar(persistenceId),
                      IsolationLevel.SNAPSHOT
                    )

      } yield Option(result).fold(0L)(_.getLong(0))
    }

  def currentEventsByTag(tag: String, offset: Option[TagOffset] = None)(implicit
    trace: Trace
  ): ZStream[Any, Throwable, PersistentRepr] =
    recordDb.streamAsyncContinuation { continue =>
      ZStream.unwrap {
        for {
          rc      <- ZIO.service[FdbRecordContext]
          evStore <- mkStore(rc)
        } yield evStore
          .scanIndexRemoteFetch(
            _.getIndex("eventTagIndex"),
            new IndexScanRange(IndexScanType.BY_VALUE, buildTagTupleRange(tag, offset)),
            continue,
            ScanProperties.FORWARD_SCAN,
            IndexOrphanBehavior.SKIP
          )
          .map(continue => convPersistentRepr(continue.record))
      }
    }

  def appendEvents(event: PersistentRepr, events: PersistentRepr*)(implicit trace: Trace): Task[Unit] =
    appendEvents(event +: events)

  def appendEvents(events: Seq[PersistentRepr])(implicit trace: Trace): Task[Unit] =
    recordDb.runAsync { rc =>
      mkStore(rc).flatMap { store =>
        ZIO
          .foreachDiscard(events.sortBy(ev => (ev.persistenceId, ev.sequenceNr))) { ev =>
            store.saveRecord(PersistentRepr.toJavaProto(ev))
          }
      }
    }

  private def eventRunLoop(
    persistenceId: String,
    toSequenceNr: Long,
    seqRef: Ref[Long]
  )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] =
    recordDb.streamAsyncContinuation { continue =>
      ZStream.unwrap {
        for {
          currentSeqNr <- seqRef.get
          rc           <- ZIO.service[FdbRecordContext]
          begin         = currentSeqNr
          store        <- mkStore(rc)
        } yield {
          val filters = List(
            Query.field("persistenceId").equalsValue(persistenceId),
            Query.and(
              Query.field("sequenceNr").greaterThanOrEquals(begin),
              Query.field("sequenceNr").lessThan(toSequenceNr)
            )
          ).asJava

          val queryPlan = RecordQuery
            .newBuilder()
            .setRecordType("PersistentRepr")
            .setFilter(
              Query.and(filters)
            )
            .build()

          store.executeQuery(queryPlan, continue).map(continuable => convPersistentRepr(continuable.record)).tap { pr =>
            seqRef.set(pr.sequenceNr + 1)
          }
        }
      }
    }

  private def buildEventsRangeQuery(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
  ) =
    Query.and(
      Query.field("persistenceId").equalsValue(persistenceId),
      Query.field("sequenceNr").greaterThanOrEquals(fromSequenceNr),
      Query.field("sequenceNr").lessThanOrEquals(toSequenceNr)
    )

  /**
   * Constructs a FoundationDB query component for retrieving events based on a
   * specific tag and optional offset.
   */
  private def buildTagTupleRange(tag: String, offset: Option[TagOffset]): TupleRange = {
    val baseTuple = Tuple.from(tag)
    val start     = offset.fold(baseTuple)(to => baseTuple.addAll(Tuple.from(to.timestamp, to.persistenceId, to.sequenceNr)))
    val et        = if (offset.isEmpty) EndpointType.RANGE_INCLUSIVE else EndpointType.RANGE_EXCLUSIVE

    new TupleRange(
      start,
      baseTuple,
      et,
      EndpointType.TREE_END
    )
  }

  private def tagRunLoop(
    tag: String,
    currentOffsetRef: Ref[Option[TagOffset]]
  )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] =
    recordDb.streamAsyncContinuation { continue =>
      ZStream.unwrap {
        for {
          currentSeqNr <- currentOffsetRef.get
          rc           <- ZIO.service[FdbRecordContext]
          store        <- mkStore(rc)
        } yield store
          .scanIndexRemoteFetch(
            _.getIndex("eventTagIndex"),
            new IndexScanRange(IndexScanType.BY_VALUE, buildTagTupleRange(tag, currentSeqNr)),
            continue,
            ScanProperties.FORWARD_SCAN,
            IndexOrphanBehavior.SKIP
          )
          .map(continue => convPersistentRepr(continue.record))
          .mapChunksZIO { chunks =>
            chunks.lastOption match {
              case Some(pr) =>
                currentOffsetRef.set(Some(TagOffset(pr.timestamp, pr.persistenceId, pr.sequenceNr))).as(chunks)
              case None     => ZIO.succeed(chunks)
            }

          }
      }
    }

  def getPrimarySplits(implicit trace: Trace): ZStream[Any, Throwable, Tuple] =
    recordDb.streamAsync {
      ZStream.unwrap {
        ZIO.serviceWithZIO[FdbRecordContext].apply { ctx =>
          for {
            es <- mkStore(ctx)
          } yield es.primaryKeyBoundaries(None)
        }
      }
    }

  def getTagSplits(implicit trace: Trace): ZStream[Any, Throwable, Tuple] =
    recordDb.streamAsync {
      ZStream.unwrap {
        ZIO.serviceWithZIO[FdbRecordContext].apply { ctx =>
          for {
            es <- mkStore(ctx)
          } yield es.indexBoundaries(_.getIndex("eventTagIndex"), None)
        }
      }
    }

  def eventsByTag(
    tag: String,
    offset: Option[TagOffset]
  )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] = {
    def getWatchKey: ZIO[Any, Throwable, Array[Byte]] = recordDb.runAsync {
      for {
        ctx   <- ZIO.service[FdbRecordContext]
        store <- mkStore(ctx)
      } yield store.indexSubspace(_.getIndex("maxTag")).pack(Tuple.from(tag))
    }

    def innerRun(
      currentOffsetRef: Ref[Option[TagOffset]]
    )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] =
      for {
        currentOffset <- ZStream.from(currentOffsetRef.get)
        _             <- ZStream.logDebug(
                           s"Starting tailing stream starting from offset=[$offset], currentOffset=[$currentOffset]"
                         )
        result        <- tagRunLoop(tag, currentOffsetRef)
      } yield result

    def outerRunWatch(
      watchKey: Array[Byte],
      currentOffset: Ref[Option[TagOffset]]
    )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] = ZStream.unwrap {
      val printable = ByteArrayUtil.printable(watchKey)

      /* Watch before we start running the computation.  We want to over watch so we
       * don't miss anything and if something comes in between when we started processing and
       * when we finished reading the stream, we'd like to know.  All it does is trigger
       * us to look again, so if its a false alarm, no problem. */
      recordDb.database.watch(watchKey).fork.map { watch =>
        ZStream.logDebug("`byPersistenceTag`.outerRunWatch - running until empty") *>
          innerRun(currentOffset) ++
          ZStream.logDebug(s"`byPersistenceTag`.innerRun complete, starting watch on key=[$printable]").drain ++
          ZStream.fromZIO(watch.await).drain ++
          ZStream.logDebug(s"watchKey triggered, new data available, key=[$printable]").drain ++
          outerRunWatch(watchKey, currentOffset)
      }
    }

    ZStream.logAnnotate(EventsourceLayer.annotateTag(tag)) *>
      ZStream
        .fromZIO(getWatchKey)
        .zip(ZStream.fromZIO(Ref.make(offset)))
        .flatMap { case (watch, currentSeqNr) =>
          outerRunWatch(watch, currentSeqNr)

        }
        .ensuring(ZIO.logDebug("Listening stream terminated"))
  }

  /** @see [[eventsById(persistenceId:String,fromSequenceNr:Long*]] */
  def eventsById(persistenceId: String)(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] =
    eventsById(persistenceId, 0L, Long.MaxValue)

  /**
   * Streams events for a specific persistenceId, starting from a specific
   * sequence number up to an optional end sequence number. Like eventsByTag, it
   * uses a watch mechanism to continue streaming events as they arrive.
   * @param fromSequenceNr
   * @param toSequenceNr
   * @return
   */
  def eventsById(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long
  )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] = {
    def getWatchKey: ZIO[Any, Throwable, Array[Byte]] = recordDb.runAsync {
      for {
        ctx   <- ZIO.service[FdbRecordContext]
        store <- mkStore(ctx)
      } yield store.indexSubspace(_.getIndex("maxSeqNr")).pack(Tuple.from(persistenceId))
    }

    def innerRun(
      currentSeqNr: Ref[Long]
    )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] = ZStream.unwrap {
      for {
        seqNr <- currentSeqNr.get
        _     <- ZIO.logDebug(
                   s"Starting tailing stream starting from fromSequenceNr=[$fromSequenceNr], currentSeqNr=[$seqNr]"
                 )
      } yield
        if (seqNr >= toSequenceNr)
          ZStream.empty
        else
          eventRunLoop(persistenceId, toSequenceNr, currentSeqNr)
    }

    def outerRunAndWatch(
      watchKey: Array[Byte],
      currentSeqNr: Ref[Long],
    )(implicit trace: Trace): ZStream[Any, Throwable, PersistentRepr] =
      ZStream.unwrapScoped {
        val printable = ByteArrayUtil.printable(watchKey)
        /* Watch before we start running the computation.  We want to over watch so we
         * don't miss anything and if something comes in between when we started processing and
         * when we finished reading the stream, we'd like to know.  All it does is trigger
         * us to look again, so if its a false alarm, no problem. */
        recordDb.database.watch(watchKey).forkScoped.map { watch =>
          ZStream.logDebug("outerRunAndWatch, started, running until empty").drain ++
            innerRun(currentSeqNr) ++
            ZStream.logDebug(s"innerRun complete, starting watch on key=[$printable]").drain ++
            ZStream.fromZIO(watch.await).drain ++
            ZStream.logDebug(s"watchKey triggered, new data available, key=[$printable]").drain ++
            outerRunAndWatch(watchKey, currentSeqNr)
        }
      }

    ZStream.unwrap {
      getWatchKey.map { watchKey =>
        ZStream.logAnnotate(EventsourceLayer.annotate(persistenceId)) *>
          ZStream
            .fromZIO(Ref.make(fromSequenceNr))
            .flatMap { currentSeqNr =>
              outerRunAndWatch(watchKey, currentSeqNr)
            }
            .ensuring(ZIO.logDebug("Listening stream terminated."))
      }
    }
  }

  /**
   * Deletes all records from the event, tag watch, and event watch stores. It's
   * marked unsafe because it should be used with caution due to its destructive
   * nature.
   */
  def unsafeDeleteAllRecords(implicit @unused unsafe: Unsafe, trace: Trace): ZIO[Any, Throwable, Unit] =
    recordDb.runAsync {
      for {
        rc <- ZIO.service[FdbRecordContext]
        _  <- ZIO.attemptBlocking(FDBRecordStore.deleteStore(rc.ctx, cfg.tablePath))
        _  <- ZIO.logWarning(s"Deleted store ${cfg.tablePath}")
        _  <- ZIO.attemptBlocking(FDBRecordStore.deleteStore(rc.ctx, cfg.metaPath))
        _  <- ZIO.logInfo(s"Deleted metaStore ${cfg.metaPath}")

      } yield ()
    }

  def saveSnapshot(ss: Snapshot, keep: Int = Int.MaxValue): ZIO[Any, Throwable, Unit] =
    recordDb.runAsync {
      for {
        rc    <- ZIO.service[FdbRecordContext]
        store <- mkStore(rc)
        _     <- store.saveRecord(Snapshot.toJavaProto(ss))
        _     <- ZIO.unless(keep == Int.MaxValue) {
                   selectSnapshot(
                     ss.persistenceId,
                     SnapshotSelectionCriteria(maxSequenceNr = ss.sequenceNr, maxTimestamp = ss.timestamp),
                     keep + 1
                   ).runCollect.flatMap { elements =>
                     ZIO.when(elements.size > keep) {
                       val exclusiveElement = elements(keep)
                       deleteSnapshot(
                         ss.persistenceId,
                         SnapshotSelectionCriteria(
                           maxSequenceNr = exclusiveElement.sequenceNr + 1,
                           maxTimestamp = exclusiveElement.timestamp + 1
                         )
                       )
                     }
                   }
                 }
      } yield ()
    }

  private def buildSnapshotRangeQuery(
    fieldName: String,
    ss: SnapshotSelectionCriteria,
    min: SnapshotSelectionCriteria => Long,
    max: SnapshotSelectionCriteria => Long,
  ) = {
    val baseQuery = Query.and(
      Query.field(fieldName).greaterThanOrEquals(min(ss)),
      Query.field(fieldName).lessThanOrEquals(max(ss))
    )

    // Constructs a FoundationDB query for snapshot deletion where the given 'fieldName'
    // falls within the range specified by 'ss' (min/max values). Handles a special case
    // where a minimum value of zero might indicate the need to include null checks.
    if (min(ss) == 0)
      Query.or(
        Query.field(fieldName).isNull,
        baseQuery
      )
    else
      baseQuery
  }

  def selectSnapshot(
    persistenceId: String,
    ss: SnapshotSelectionCriteria,
    limit: Int
  ): ZStream[FdbRecordContext, Throwable, Snapshot] = selectSnapshot(persistenceId, null, ss, limit)

  /**
   * Retrieves a stream of the most recent snapshot for a given persistent
   * entity.
   *
   * This method fetches the latest snapshot that matches the provided
   * `SnapshotSelectionCriteria` for a persistent entity identified by
   * `persistenceId`. It constructs a query to retrieve the snapshot records
   * from a record database based on the criteria: sequence number range and
   * timestamp range. Only the most recent snapshot within the specified
   * criteria is streamed back.
   *
   * @param persistenceId
   *   the unique identifier of the persistent entity for which the snapshot is
   *   to be retrieved.
   * @param ss
   *   the criteria used to select the snapshot, including the range of sequence
   *   numbers and timestamps.
   * @return
   *   a `ZStream[Any, Throwable, Snapshot]` which upon subscription, will
   *   provide a stream of snapshots. The stream is expected to emit either a
   *   single snapshot if a matching one is found or no snapshots if none match
   *   the criteria. It can also fail with a Throwable if there's an issue
   *   accessing the database.
   *
   * Internally, it utilizes the `ZStream.unwrap` method to asynchronously
   * execute the database query within a for-comprehension, handling resource
   * acquisition and release. The actual query filters snapshots based on the
   * `persistenceId` and the range conditions derived from
   * `SnapshotSelectionCriteria`. The snapshots are sorted by sequence number in
   * ascending order, and only the most recent snapshot (the first result of the
   * query) is streamed back. The retrieved database records are then converted
   * to `Snapshot` objects using the `convSnapshot` function.
   */
  def selectSnapshot(
    persistenceId: String,
    continuation: Array[Byte],
    ss: SnapshotSelectionCriteria,
    limit: Int
  ): ZStream[FdbRecordContext, Throwable, Snapshot] =
    ZStream.unwrap {

      for {
        rc    <- ZIO.service[FdbRecordContext]
        store <- mkStore(rc)
        query <- ZIO.succeed {
                   val seqNr = buildSnapshotRangeQuery("sequenceNr", ss, _.minSequenceNr, _.maxSequenceNr)
                   val ts    = buildSnapshotRangeQuery("timestamp", ss, _.minTimestamp, _.maxTimestamp)

                   RecordQuery
                     .newBuilder()
                     .setRecordType("Snapshot")
                     .setFilter(
                       Query.and(
                         Query.field("persistenceId").equalsValue(persistenceId),
                         seqNr,
                         ts
                       )
                     )
                     .setSort(
                       Key.Expressions.field("sequenceNr"),
                       true
                     )
                     .build()
                 }
      } yield store
        .executeQuery(query, continuation, ExecuteProperties.newBuilder().setScannedRecordsLimit(limit).build())
        .map(continuable => convSnapshot(continuable.record))
    }

  def selectLatestSnapshot(persistenceId: String, ss: SnapshotSelectionCriteria): ZStream[Any, Throwable, Snapshot] =
    recordDb.streamAsyncContinuation { continue =>
      selectSnapshot(persistenceId, continue, ss, 1)
    }

  /**
   * Deletes a single snapshot from the event store based on its persistenceId
   * and sequenceNr. This method creates a transaction.
   */
  def deleteSingleSnapshot(persistenceId: String, sequenceNr: Long): ZIO[Any, Throwable, Unit] =
    recordDb.runAsync {
      for {
        rc    <- ZIO.service[FdbRecordContext]
        store <- mkStore(rc)
        query  = {
          val seqNr = buildSnapshotRangeQuery(
            "sequenceNr",
            SnapshotSelectionCriteria(maxSequenceNr = sequenceNr, minSequenceNr = sequenceNr),
            _.minSequenceNr,
            _.maxSequenceNr
          )

          RecordQuery
            .newBuilder()
            .setRecordType("Snapshot")
            .setFilter(
              Query.and(
                Query.field("persistenceId").equalsValue(persistenceId),
                seqNr,
              )
            )
            .build()
        }
        _     <- store
                   .executeQuery(query)
                   .mapZIOPar(4) { qr =>
                     store.deleteRecords(qr.record.getPrimaryKey)
                   }
                   .runDrain
      } yield ()
    }

  def deleteSnapshot(persistenceId: String, ss: SnapshotSelectionCriteria): ZIO[Any, Throwable, Unit] =
    recordDb.runAsync {
      for {
        rc    <- ZIO.service[FdbRecordContext]
        store <- mkStore(rc)
        query  = {
          val seqNr = buildSnapshotRangeQuery("sequenceNr", ss, _.minSequenceNr, _.maxSequenceNr)
          val ts    = buildSnapshotRangeQuery("timestamp", ss, _.minTimestamp, _.maxTimestamp)

          RecordQuery
            .newBuilder()
            .setRecordType("Snapshot")
            .setFilter(
              Query.and(
                Query.field("persistenceId").equalsValue(persistenceId),
                seqNr,
                ts,
              )
            )
            .build()
        }
        _     <- store
                   .executeQuery(query)
                   .mapZIOPar(4) { qr =>
                     store.deleteRecords(qr.record.getPrimaryKey)
                   }
                   .runDrain
      } yield ()
    }

  def totalRecordCount: Task[Long] = recordDb.runAsync { rc =>
    for {
      store <- mkStore(rc)
      count <- store.snapshotRecordCount
    } yield count
  }

  def totalRecordCountByType(recordType: String): Task[Long] = recordDb.runAsync { rc =>
    for {
      store <- mkStore(rc)
      count <- store.snapshotRecordCountForRecordType(recordType)
    } yield count
  }

}

object EventsourceLayer {
  private[fdb] val PersistenceId = "persistenceId"
  private[fdb] val Tag           = "tag"

  private def annotate(id: String): LogAnnotation     = LogAnnotation(PersistenceId, id)
  private def annotateTag(tag: String): LogAnnotation = LogAnnotation(Tag, tag)

  /** Accounting helper */
  case class TagOffset(timestamp: Long, persistenceId: String, sequenceNr: Long)

  /**
   * Represents criteria for selecting snapshots from an event store, using
   * sequence numbers and timestamps.
   *
   * @param maxSequenceNr
   *   inclusive
   * @param maxTimestamp
   *   inclusive
   * @param minSequenceNr
   *   inclusive
   * @param minTimestamp
   *   inclusive
   */
  final case class SnapshotSelectionCriteria(
    maxSequenceNr: Long = Long.MaxValue,
    maxTimestamp: Long = Long.MaxValue,
    minSequenceNr: Long = 0L,
    minTimestamp: Long = 0L
  )

  case class EventsourceConfig(
    keyspace: KeySpace,
    tablePath: KeySpacePath,
    metaPath: KeySpacePath,
    persistLocalMetadata: Boolean,
  ) extends RecordKeySpace.RecordKeySpaceLike

  object EventsourceConfig {
    def makeDefaultConfig(parent: KeySpaceDirectory): EventsourceConfig =
      makeDefaultConfig(parent, persistLocalMetadata = true)

    def makeDefaultConfig(parent: KeySpaceDirectory, persistLocalMetadata: Boolean): EventsourceConfig = {
      val record = RecordKeySpace.makeDefaultConfig(parent)
      EventsourceConfig(
        keyspace = record.keyspace,
        tablePath = record.tablePath,
        metaPath = record.metaPath,
        persistLocalMetadata = persistLocalMetadata
      )
    }
  }

  val live: ZLayer[EventsourceConfig & FdbRecordDatabaseFactory, Throwable, EventsourceLayer] = ZLayer.scoped {
    for {
      c        <- ZIO.service[EventsourceConfig]
      factory  <- ZIO.service[FdbRecordDatabaseFactory]
      metadata <- Ref.make(Option.empty[FdbMetadata])
      realDb   <- factory.db
      _        <-
        ZIO.logInfo(
          s"Provisioned eventStore system under keyspace=[${c.keyspace}], table=[${c.tablePath}], meta=[${c.metaPath}]"
        )
    } yield new EventsourceLayer(
      realDb,
      RecordConfig(
        recordKeySpace = c,
        localMetadata = FdbMetadata(EventsourceMeta.buildMetadata.build(), c.tablePath),
        localFileDescriptor = EventsourceMeta.descriptor,
        persistLocalMetadata = c.persistLocalMetadata
      ),
      metadata
    )
  }
}
