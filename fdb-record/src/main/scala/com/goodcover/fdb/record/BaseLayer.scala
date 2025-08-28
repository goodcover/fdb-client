package com.goodcover.fdb.record

import com.apple.foundationdb.record.provider.foundationdb.{ FDBMetaDataStore, FDBRecordStore }
import com.goodcover.fdb.record.RecordDatabase.{ FdbMetadata, FdbRecordContext, FdbRecordDatabase, FdbRecordStore }
import zio.*

import scala.annotation.unused
import scala.jdk.CollectionConverters.CollectionHasAsScala

class BaseLayer(
  protected val recordDb: FdbRecordDatabase,
  protected val cfg: RecordConfig,
  protected val metaRef: Ref[Option[FdbMetadata]]
) {
  protected def getMetaData(implicit trace: Trace): UIO[FdbMetadata] =
    for {
      result <-
        metaRef.get.some
          .orElse(loadMetadata.some)
          .orElseFail(cfg.localMetadata)
          .merge
    } yield result

  protected def loadMetadata(implicit trace: Trace): Task[Option[FdbMetadata]] =
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

  def mkStore(rc: FdbRecordContext)(implicit trace: Trace): Task[FdbRecordStore] =
    getMetaData.flatMap { meta =>
      rc.createOrOpen(meta)
    }

  /**
   * Saves the metadata to the specified path. If metadata is not loaded, it
   * will save and set the current metadata. If metadata is already loaded, it
   * will update the existing records.
   *
   * @param trace
   *   implicit trace for logging.
   * @return
   *   a Task that completes when the metadata is saved.
   */
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
   * Executes a given function with an FdbRecordStore.
   *
   * @param fn
   *   The function to execute, which takes an FdbRecordStore and returns a RIO.
   * @param trace
   *   Implicit trace for logging.
   * @tparam R
   *   The environment type of the RIO.
   * @tparam A
   *   The result type of the RIO.
   * @return
   *   A ZIO that executes the function with the store.
   */
  def withStoreTxn[R, A](fn: FdbRecordStore => RIO[FdbRecordContext with R, A])(implicit trace: Trace): ZIO[R, Throwable, A] =
    recordDb.runAsync[R, A] { (ctx: FdbRecordContext) =>
      mkStore(ctx).flatMap(store => fn(store))
    }

  def totalRecordCount: Task[Long] = withStoreTxn { store =>
    store.snapshotRecordCount.map(_.toLong)
  }

  def totalRecordCountByType(recordType: String): Task[Long] = withStoreTxn { store =>
    store.snapshotRecordCountForRecordType(recordType).map(_.toLong)
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
}
