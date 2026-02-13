package com.goodcover.fdb

import com.apple.foundationdb.directory.{ DirectoryLayer, DirectorySubspace }
import com.apple.foundationdb.tuple.Versionstamp
import com.apple.foundationdb.{
  Database,
  FDB,
  FDBException,
  KeySelector,
  KeyValue,
  LocalityUtil,
  MutationType,
  Range as FDBRange,
  ReadTransaction,
  StreamingMode,
  Transaction,
  TransactionOptions
}
import com.goodcover.fdb.FdbDatabase.Transactor
import zio.Unsafe.*
import zio.ZIO.LogAnnotate
import zio.*
import zio.stream.ZStream

import java.io.File
import java.util.UUID
import java.util.concurrent.{ CompletableFuture, Executor, TimeUnit }

/**
 * Represents the way to get a DbConnection. Honestly not sure how useful this
 * is since the client does a lot of work to initialize itself, and it uses a
 * lot of singletons.
 */
abstract class FdbPool {
  def transact: ZLayer[Any, Throwable, FdbDatabase]
  def config: FoundationDbConfig
  def invalidate(conn: FdbDatabase): UIO[Any]
  def fdb: FDB
}

object FdbPool {

  //////////////
  /// Constants
  //////////////

  /**
   * An ENV Variable that sets the Java property on startup, this makes it
   * possible to configure the underlying java library from an environment
   * variable.
   */
  val FDB_LIBRARY_PATH_FDB_C = "FDB_LIBRARY_PATH_FDB_C"

  /**
   * An ENV variable to override the cluster file path, even if you set it in
   * the config it will win if it's specified.
   */
  val FDB_CLUSTER_FILE = "FDB_CLUSTER_FILE"

  private def findFileRecursive(startDir: File, fileName: String): Option[File] = {
    def innerFind(dir: File): Option[File] = {
      val matchingFile = dir.listFiles().find(_.getName == fileName)
      matchingFile match {
        case Some(file) => Some(file) // Found the file!
        case None       =>
          dir.getParentFile match {
            case null      => None                 // Reached the root without finding the file
            case parentDir => innerFind(parentDir) // Recurse upwards
          }
      }
    }

    innerFind(startDir)
  }

  /**
   * The problem is that with sbt we share class loaders which load jni
   * libraries.
   *
   * The java code gets collected but the dynamic library doesn't get unlinked.
   * The client still thinks its connected (because it is). So we simulate it
   * working again and we do everything together and we'd just Die if there's a
   * problem.
   *
   * This basically assumes you've set everything up okay.
   */
  private def handleFDBAlreadyRunning(version: Int) = synchronized {
    // Re-instantiate the constructor that's already linked to the library
    val constructor = classOf[FDB].getDeclaredConstructor(classOf[Int])
    constructor.setAccessible(true)
    val fdb         = constructor.newInstance(version)

    // Set the network as started.
    handleNetworkAlreadySetup(fdb): Unit

    // Set the singleton to this instance
    val singleton = classOf[FDB].getDeclaredField("singleton")
    singleton.setAccessible(true)
    singleton.set(null, fdb)
    fdb
  }

  private def setFDBCSystemProperty(value: String) =
    java.lang.System.setProperty(FDB_LIBRARY_PATH_FDB_C, value)

  private def searchClusterFile(cfg: FoundationDbConfig): ZIO[Any, Nothing, Option[String]] = {

    val searchedCluster =
      if (cfg.searchClusterFile)
        ZIO.succeed {
          sys.props
            .get("user.dir")
            .flatMap(userDir => findFileRecursive(new File(userDir), "fdb.cluster"))
            .map(_.getAbsolutePath)
        }.flatMap {
          case Some(value) => ZIO.logDebug(s"Found config via searching, $value").as(Option(value))
          case None        => ZIO.none
        }
      else ZIO.none

    searchedCluster.map { searched =>
      sys.env
        .get(FDB_CLUSTER_FILE) // Env priority
        .orElse(cfg.clusterFile) // Config file next
        .orElse(searched) // Searched config file
    }
  }

  /**
   * Less important than above since it usually works itself out, however, we
   * force it so we get feedback earlier if something is going to go haywire
   */
  private def handleNetworkAlreadySetup(fdb: FDB) = synchronized {
    // Set the network as started.
    val netStarted = classOf[FDB].getDeclaredField("netStarted")
    netStarted.setAccessible(true)
    netStarted.set(fdb, true)
    fdb
  }

  def make()(implicit trace: Trace): ZLayer[FoundationDbConfig, Throwable, FdbPool] = ZLayer.scoped {
    for {
      fdbConfig      <- ZIO.service[FoundationDbConfig]
      _              <- ZIO.attemptBlocking(if (fdbConfig.setFdbCLibraryPath.nonEmpty) {
                          setFDBCSystemProperty(fdbConfig.setFdbCLibraryPath.get)
                        })
      _              <- ZIO.attemptBlocking(if (fdbConfig.getFdbCLibraryFromEnv) {
                          sys.env.get(FDB_LIBRARY_PATH_FDB_C).foreach(setFDBCSystemProperty)
                        })
      fb             <- ZIO.attemptBlocking {
                          FDB.selectAPIVersion(fdbConfig.apiVersion)
                        }.catchSome {
                          // API version may be set only once
                          case fdb: FDBException if fdb.getCode == 2201 && fdbConfig.performClassLoaderFixes =>
                            ZIO.attemptBlocking(handleFDBAlreadyRunning(fdbConfig.apiVersion))
                        }
      _              <- ZIO.logDebug(s"Selected version, $fb, version=[${fdbConfig.apiVersion}], disabling shutdownHook")
      _              <- ZIO.attempt(fb.disableShutdownHook())
      _              <- ZIO.attemptBlocking(fb.startNetwork()).catchSome[Any, Throwable, Any] {
                          // Network may only be started once, again a classpath issue, since java no-ops if it is.
                          case fdb: FDBException if fdb.getCode == 2009 && fdbConfig.performClassLoaderFixes =>
                            ZIO.attemptBlocking(handleNetworkAlreadySetup(fb))
                        }
      _              <- ZIO.attempt(fb.setUnclosedWarning(fdbConfig.warnOnUnclosed))
      _              <- ZIO.logDebug(s"Network started, shutdown hook disabled.")
      id             <- Ref.make(0L)
      openTxn        <- Ref.make(Set.empty[Long])
      maybeCF        <- searchClusterFile(fdbConfig)
      outputFdbConfig = fdbConfig.withClusterFile(maybeCF)
      acquire         = ZIO.attemptBlocking {
                          outputFdbConfig.fdbExecutor match {
                            case Some(value) => fb.open(maybeCF.orNull, value)
                            case None        => fb.open(maybeCF.orNull, Runtime.defaultBlockingExecutor.asJava)
                          }

                        } <* ZIO.logDebug(s"[DB] step=1, Acquired a new db clusterfile=[$maybeCF]")
      getDb           =
        ZIO.acquireRelease(acquire.retry(fdbConfig.retryPolicy).map(new FdbDatabase(_, id, openTxn)))(_.close.ignoreLogged)
      pool           <- ZPool.make(getDb, Range(fdbConfig.minConnections, fdbConfig.maxConnections), fdbConfig.timeToLive)
      tx              = ZLayer.scoped[Any] {
                          for {
                            db <- pool.get
                            _  <- ZIO.addFinalizerExit {
                                    case Exit.Success(_)     =>
                                      ZIO.logDebug("[DB] step=[2], Returning the database to the pool").unit *> db.close.orDie
                                    case Exit.Failure(cause) =>
                                      ZIO.logCause(cause) *> db.close.orDie
                                  }
                          } yield db
                        }
    } yield new FdbPool {
      override def transact: ZLayer[Any, Throwable, FdbDatabase] = tx

      override def config: FoundationDbConfig = outputFdbConfig

      override def fdb: FDB = fb

      override def invalidate(conn: FdbDatabase): UIO[Any] = pool.invalidate(conn)
    }
  }

}

/**
 * The top level interesting class. The database which is a layer over the
 * [[Database]] object from their C client library.
 *
 * We manage transactions through the [[transaction]] function. This provides
 * the lifecycle in ZIO land of how long a transaction will exist. This makes it
 * pretty easy to reason about when stuff is reused or not.
 *
 * We also wrap some DirectoryLayer calls in here because its natural.
 *
 * The one difference from the api is [[watch]], watches are interesting but
 * practically you need to know how to use them. A watch will only exist outside
 * a transaction when someone calls commit. We use the [[Database#runAsync]] so
 * it usually holds the transaction open as long as the [[FdbTxn]] exists, but
 * this doesn't make as much sense for watches. So we offer an alternative here,
 * somewhat described here
 * [[https://forums.foundationdb.org/t/understanding-watches/903/10]]
 */
final class FdbDatabase(val db: Database, idRef: Ref[Long], openTransactions: Ref[Set[Long]]) {

  import scala.jdk.CollectionConverters.*

  private val directoryLayer = new DirectoryLayer()

  /** Close the database, mainly handled in ZLayer, so rarely needed. */
  def close: Task[Unit] = ZIO.attempt(db.close())

  def stream(implicit trace: Trace): ZLayer[Any, Nothing, FdbStream] = ZLayer {
    ZIO.succeed(new FdbStream(this))
  }

  /**
   * Wrapper around the database call with the same name.
   *
   * We inject some zio goodness here, part of that is internal bookeeping on
   * how many transactions are open, and specifically what transaction is open.
   * It also handles the lifecycle as part of the scope, along with attaching
   * some annotations to the log file.
   */
  def runAsync[R, A](
    fn: => ZIO[FdbTxn & R, Throwable, A]
  )(implicit trace: Trace): ZIO[R, Throwable, A] =
    unsafe { implicit unsafe =>
      ZIO.scoped[R] {
        for {
          runtime  <- ZIO.runtime[R]
          id       <- idRef.updateAndGet(_ + 1)
          _        <- recordOpenTransaction(id) &> ZIO.addFinalizer(recordClosedTransaction(id))
          refCount <- Ref.make(0)
          result   <- ZIO.fromCompletableFuture(db.runAsync { txn =>
                        val cf   = new CompletableFuture[A]()
                        val fork = runtime.unsafe.fork {
                          FdbDatabase.logTxnId(id) {
                            ZIO.blocking {
                              fn.provideSomeEnvironment[R](_.add[FdbTxn](new FdbTxn(new Transactor(txn, id), refCount)))
                            }
                          }
                        }

                        fork.unsafe.addObserver {
                          case Exit.Success(value)                    => cf.complete(value): Unit
                          case Exit.Failure(c) if c.isInterruptedOnly => cf.cancel(true): Unit
                          case Exit.Failure(c)                        => cf.completeExceptionally(c.squash): Unit
                        }

                        cf

                      })
        } yield result
      }
    }

  def directoryCreateOrOpen(path: Seq[String]): Task[DirectorySubspace] =
    ZIO.fromCompletableFuture(directoryLayer.createOrOpen(db, path.asJava))

  def subDirectoryCreateOrOpen(d: DirectorySubspace, path: Seq[String]): Task[DirectorySubspace] =
    ZIO.fromCompletableFuture(d.createOrOpen(db, path.asJava))

  /**
   * The most non-standard
   */
  def watch(key: Array[Byte]): ZIO[Any, Throwable, Unit] =
    ZIO
      .fromCompletableFuture(db.runAsync { txn =>
        CompletableFuture.completedFuture(txn.watch(key))
      })
      .flatMap(ZIO.fromCompletableFuture(_))
      .unit

  private[fdb] def recordOpenTransaction(id: Long) = openTransactions.update(_ + id)

  private[fdb] def recordClosedTransaction(id: Long) = openTransactions.update(_ - id)

  private[fdb] def getOpenTransactions = openTransactions.get

}

object FdbDatabase {

  /**
   * Simple case class for holding the native transaction with an identifier.
   * Used internally.
   */
  private[fdb] class Transactor[T](
    internalTransaction: T,
    id: Long
  ) {
    def txn: T = this.internalTransaction

    def txnId: Long = id

    def annotate: LogAnnotation = LogAnnotation(Transactor.TxId, txnId.toString)

  }

  private[fdb] object Transactor {
    private[fdb] val TxId = "txId"
  }

  private def logTxnId(id: Long): LogAnnotate =
    ZIO.logAnnotate(Transactor.TxId, id.toString)

  def watch(key: Array[Byte])(implicit trace: Trace): ZIO[FdbDatabase, Throwable, Unit] =
    ZIO.serviceWithZIO[FdbDatabase](_.watch(key))

  val layer: ZLayer[FdbPool, Throwable, FdbDatabase] =
    ZLayer(ZIO.serviceWith[FdbPool](_.transact)).flatten

  def runAsync[R, A](
    fn: => ZIO[FdbTxn & R, Throwable, A]
  )(implicit trace: Trace): ZIO[FdbDatabase & R, Throwable, A] =
    ZIO.serviceWithZIO[FdbDatabase](_.runAsync[R, A](fn))
}

final class FdbTxn(
  private[fdb] val transactor: Transactor[Transaction],
  userVersionRef: Ref[Int],
) {

  /** See [[com.apple.foundationdb.Transaction#clear(byte[])]] */
  def close: Task[Unit] = ZIO.attempt(transactor.txn.close())

  /** See [[com.apple.foundationdb.Transaction#set(byte[], byte[])]] */
  def set(k: Array[Byte], value: Array[Byte])(implicit trace: Trace): Task[Unit] =
    ZIO.attempt(transactor.txn.set(k, value))

  def clear(r: FDBRange): Task[Unit] = ZIO.attempt(transactor.txn.clear(r))

  def clear(key: Array[Byte]): Task[Unit] = ZIO.attempt(transactor.txn.clear(key))

  def clear(begin: Array[Byte], end: Array[Byte]): Task[Unit] = ZIO.attempt(transactor.txn.clear(begin, end))

  /**
   * Get a value return None on a null result
   */
  def get(k: Array[Byte])(implicit trace: Trace): Task[Option[Array[Byte]]] =
    ZIO.fromCompletableFuture(transactor.txn.get(k)).map(Option(_))

  /**
   * Read about the options to [[MutationType]] and this operation at
   * [[Transaction#mutate(com.apple.foundationdb.MutationType, byte[], byte[])]].
   */
  def mutate(optype: MutationType, key: Array[Byte], param: Array[Byte])(implicit trace: Trace): Task[Unit] =
    ZIO.attempt(transactor.txn.mutate(optype, key, param))

  /** Explicitly call commit, this shouldn't really be used */
  def commit()(implicit trace: Trace): Task[Void] =
    ZIO.fromCompletableFuture(transactor.txn.commit())

  /** Get the version stamp responsible for this particular transaction. */
  def versionstamp: ZIO[Any, Throwable, Option[Versionstamp]] =
    ZIO.fromCompletableFuture(transactor.txn.getVersionstamp).map(Option(_)).map(_.map(Versionstamp.fromBytes))

  /**
   * This is an interesting addition, essentially if you are committing keys
   * with [[com.apple.foundationdb.tuple.Versionstamp]] you can use this number
   * in the scope of a transaction to handle order within a transaction.
   *
   * It doesn't automatically increment, you have to increment it yourself (by
   * calling this function), but it still gives you a pretty reliable
   * transaction oriented way to update a number to preserve order.
   */
  def userVersion: UIO[Int] = userVersionRef.getAndUpdate(_ + 1)

  def annotate: LogAnnotate = ZIO.logAnnotate(transactor.annotate)

  def options(fn: TransactionOptions => Unit): ZIO[Any, Nothing, Unit] =
    ZIO.succeed(fn(transactor.txn.options()))

  def boundaryKeys(begin: Array[Byte], end: Array[Byte]): ZIO[Any, Throwable, NonEmptyChunk[FdbTxn.Boundary]] =
    ZIO.scoped {
      for {
        iter              <- ZIO.succeed(LocalityUtil.getBoundaryKeys(transactor.txn, begin, end))
        _                 <- ZIO.addFinalizer(ZIO.attempt(iter.close()).orDie)
        keyLocalityRanges <- ZStream
                               .unfoldZIO(iter) { iterator =>
                                 ZIO.fromCompletableFuture(iterator.onHasNext()).flatMap {
                                   case java.lang.Boolean.TRUE => ZIO.some((iterator.next(), iterator))
                                   case _                      => ZIO.none
                                 }
                               }
                               .runCollect

        keyLocalityRanges <-
          if (keyLocalityRanges.nonEmpty && !java.util.Arrays.equals(keyLocalityRanges.head, begin)) {
            ZIO.succeed(Chunk(begin) ++ keyLocalityRanges)
          } else {
            ZIO.succeed(keyLocalityRanges)
          }

        splitRange     = if (keyLocalityRanges.nonEmpty) {
                           NonEmptyChunk
                             .fromChunk(keyLocalityRanges.zip(keyLocalityRanges.drop(1) ++ List(end)).zipWithIndex.map {
                               case ((start, end), i) =>
                                 (new FDBRange(start, end), i)
                             })
                             .getOrElse(
                               throw new IllegalArgumentException("This should never happen, there should always be a chunk")
                             )
                         } else NonEmptyChunk.single((new FDBRange(begin, end), 0))
        addressesPair <- ZIO.foreach(splitRange) { case (range, i) =>
                           ZIO
                             .fromCompletableFuture(LocalityUtil.getAddressesForKey(transactor.txn, range.begin))
                             .map(arr => Chunk.fromArray(arr) -> (range, i))
                         }
        withEstimate  <- ZIO.foreach(addressesPair) { case (addresses, (range, i)) =>
                           estimatedRangeSizeBytes(range).map { bytes =>
                             FdbTxn.Boundary(i, addresses, range, bytes)
                           }
                         }
      } yield withEstimate
    }

  def approximateSize: Task[Long] = ZIO.fromCompletableFuture(transactor.txn.getApproximateSize).map(_.longValue())

  /** Estimate the amount of data in a range */
  def estimatedRangeSizeBytes(begin: Array[Byte], end: Array[Byte]): Task[Long] =
    ZIO.fromCompletableFuture(transactor.txn.getEstimatedRangeSizeBytes(begin, end)).map(_.toLong)

  def estimatedRangeSizeBytes(range: FDBRange): Task[Long] =
    ZIO.fromCompletableFuture(transactor.txn.getEstimatedRangeSizeBytes(range)).map(_.toLong)

}

object FdbTxn {

  /** Boundary returned by key divider */
  case class Boundary(seqNr: Int, addresses: Chunk[String], range: FDBRange, estimatedSize: Long)

  private[fdb] val emptyUuid = new UUID(0L, 0L)

  def execute[R, E, A](fn: FdbTxn => ZIO[R, E, A]): ZIO[FdbTxn & R, E, A] =
    for {
      txn    <- ZIO.service[FdbTxn]
      result <- fn(txn)
    } yield result

  def get(k: Array[Byte])(implicit trace: Trace): ZIO[FdbTxn, Throwable, Option[Array[Byte]]] =
    ZIO.serviceWithZIO[FdbTxn](_.get(k))

  def set(k: Array[Byte], value: Array[Byte])(implicit trace: Trace) =
    ZIO.serviceWithZIO[FdbTxn](_.set(k, value))

  def close(implicit trace: Trace): ZIO[FdbTxn, Throwable, Unit] =
    ZIO.serviceWithZIO[FdbTxn](_.close)

  def clear(r: FDBRange): ZIO[FdbTxn, Throwable, Unit] =
    ZIO.serviceWithZIO[FdbTxn](_.clear(r))

  def clear(key: Array[Byte]): ZIO[FdbTxn, Throwable, Unit] =
    ZIO.serviceWithZIO[FdbTxn](_.clear(key))

  def options(fn: TransactionOptions => Unit): ZIO[FdbTxn, Nothing, Unit] =
    ZIO.serviceWithZIO[FdbTxn](_.options(fn))

  def boundaryKeys(
    begin: Array[Byte],
    end: Array[Byte]
  ): ZIO[FdbTxn, Throwable, NonEmptyChunk[Boundary]] =
    ZIO.serviceWithZIO[FdbTxn](_.boundaryKeys(begin, end))

  def approximateSize: ZIO[FdbTxn, Throwable, Long] =
    ZIO.serviceWithZIO[FdbTxn](_.approximateSize)

  def estimatedRangeSizeBytes(
    begin: Array[Byte],
    end: Array[Byte]
  ): ZIO[FdbTxn, Throwable, Long] =
    ZIO.serviceWithZIO[FdbTxn](_.estimatedRangeSizeBytes(begin, end))

  def estimatedRangeSizeBytes(
    range: FDBRange,
  ): ZIO[FdbTxn, Throwable, Long] =
    ZIO.serviceWithZIO[FdbTxn](_.estimatedRangeSizeBytes(range))
}

final class FdbStream(db: FdbDatabase) {

  private def snapshotDecorator(isSnapshot: Boolean)(t: Transaction): ReadTransaction =
    if (isSnapshot) t.snapshot()
    else t

  def runAsync[R, A](
    fn: => ZStream[FdbTxn & R, Throwable, A]
  )(implicit trace: Trace): ZStream[R, Throwable, A] =
    ZStream.unwrapScoped[R] {
      for {

        completer <- Promise.make[Throwable, Unit]
        txn       <- Promise.make[Throwable, FdbTxn]
        _         <- db.runAsync[Any, Unit](ZIO.serviceWithZIO[FdbTxn](f => txn.succeed(f) *> completer.await)).forkScoped
        txFilled  <- txn.await
        _         <- ZIO.addFinalizerExit(e =>
                       completer.done(
                         e.mapErrorExit(e => new IllegalStateException(s"Failed for an unknown reason, $e")).mapExit(_ => ())
                       )
                     )
      } yield fn.provideSomeEnvironment[R](_.add[FdbTxn](txFilled))

    }

  /**
   * Pretty much a passthrough to
   * [[com.apple.foundationdb.ReadTransaction#getRange(com.apple.foundationdb.KeySelector, com.apple.foundationdb.KeySelector, int, boolean, com.apple.foundationdb.StreamingMode)]]
   * but using our scoping and transactional structure.
   */
  def rangeNoRetry(
    start: KeySelector,
    end: KeySelector,
    limit: Int = ReadTransaction.ROW_LIMIT_UNLIMITED,
    reverse: Boolean = false,
    mode: StreamingMode = StreamingMode.WANT_ALL,
    isSnapshot: Boolean = true
  ): ZStream[FdbTxn, Throwable, KeyValue] = ZStream.unwrap {
    for {
      txn    <- ZIO.service[FdbTxn]
      result <- ZIO.attemptBlocking {
                  val iterator =
                    snapshotDecorator(isSnapshot)(txn.transactor.txn)
                      .getRange(start, end, limit, reverse, mode)
                      .iterator()
                  ZStream.unfoldZIO(iterator) { iterator =>
                    ZIO.fromCompletableFuture(iterator.onHasNext()).flatMap {
                      case java.lang.Boolean.TRUE => ZIO.some((iterator.next(), iterator))
                      case _                      => ZIO.none
                    }
                  }
                }
    } yield result
  }

  /**
   * Call getRange on a new transaction created in this function. We control the
   * transaction here vs some of the other methods on [[FdbTxn]] because we want
   * more precise control on the timeout issue. Timeout being code 1007 which
   * specifies you can't hold a transaction alive for too long.
   *
   * We also specifically call
   * [[com.apple.foundationdb.ReadTransaction#snapshot()]] to relax some of the
   * requirements around conflicts
   */
  def range(
    start: KeySelector,
    end: KeySelector,
    limit: Int = ReadTransaction.ROW_LIMIT_UNLIMITED,
    reverse: Boolean = false,
    mode: StreamingMode = StreamingMode.WANT_ALL,
    isSnapshot: Boolean = true
  ): ZStream[Any, Throwable, KeyValue] = {

    def isUnlimited = limit == ReadTransaction.ROW_LIMIT_UNLIMITED

    def acquire(start: KeySelector, end: KeySelector, limitRef: Ref[Int]) =
      for {
        transactor <- ZIO.serviceWith[FdbTxn](_.transactor)
        limit      <- limitRef.get
        result     <-
          ZIO
            .attempt(
              snapshotDecorator(isSnapshot)(transactor.txn).getRange(start, end, limit, reverse, mode).iterator()
            )
            .map(_ -> transactor)
      } yield result

    def handleRestart(lastSeenElement: Ref[KeyValue], limit: Ref[Int], fdb: FDBException, c: Cause[Any]) =
      ZStream.unwrap {
        for {
          startingElement <- lastSeenElement.get
          _               <- ZIO.logWarningCause(s"Restarting stream because we got error, $fdb", c)

        } yield runStream(KeySelector.firstGreaterOrEqual(startingElement.getKey), end, lastSeenElement, limit).drop(1)
      }

    def updateLimit(limit: Ref[Int]) =
      limit.update {
        case x if isUnlimited => x
        case x                => x - 1
      }

    def runStream(
      start: KeySelector,
      end: KeySelector,
      lastSeenElement: Ref[KeyValue],
      limit: Ref[Int]
    ): ZStream[Any, Throwable, KeyValue] =
      runAsync[Any, KeyValue] {
        ZStream.unwrapScoped {
          for {
            pair                  <- acquire(start, end, limit)
            (iterator, transactor) = pair
            _                     <- ZIO.addFinalizer(ZIO.attempt(iterator.cancel()).orDie)
          } yield ZStream.logAnnotate(transactor.annotate) *> ZStream
            .unfoldZIO(iterator) { _ =>
              ZIO
                .fromCompletableFuture(iterator.onHasNext())
                .flatMap {
                  case java.lang.Boolean.TRUE =>
                    ZIO.attempt(iterator.next()).flatMap { value =>
                      (lastSeenElement.set(value) *> updateLimit(limit)).as(Some((value, iterator)))
                    }
                  case _                      =>
                    ZIO.attempt(iterator.cancel()).as(None)
                }

            }
            .catchSomeCause {
              case c @ Cause.Die(fdb: FDBException, _) if fdb.getCode == 1007  =>
                handleRestart(lastSeenElement, limit, fdb, c)
              case c @ Cause.Fail(fdb: FDBException, _) if fdb.getCode == 1007 =>
                handleRestart(lastSeenElement, limit, fdb, c)
            }
        }
      }

    val stream = for {
      lastSeenElement <- Ref.make[KeyValue](null)
      limitToGo       <- Ref.make(limit)
    } yield runStream(start, end, lastSeenElement, limitToGo)

    ZStream
      .unwrap(stream)
  }

}

object FdbStream {

  /** Given a FdbDatabase, give me a stream */
  val layer: ZLayer[FdbDatabase, Nothing, FdbStream] = ZLayer.scoped {
    ZIO.serviceWith[FdbDatabase](_.stream)
  }.flatten

  /** @see [[FdbStream.range()]] */
  def range(
    start: KeySelector,
    end: KeySelector,
    limit: Int = ReadTransaction.ROW_LIMIT_UNLIMITED,
    reverse: Boolean = false,
    mode: StreamingMode = StreamingMode.WANT_ALL,
    isSnapshot: Boolean = true
  ): ZStream[FdbStream, Throwable, KeyValue] =
    ZStream.serviceWith[FdbStream](_.range(start, end, limit, reverse, mode, isSnapshot)).flatten

  /** @see [[FdbStream.rangeNoRetry()]] */
  def rangeNoRetry(
    start: KeySelector,
    end: KeySelector,
    limit: Int = ReadTransaction.ROW_LIMIT_UNLIMITED,
    reverse: Boolean = false,
    mode: StreamingMode = StreamingMode.WANT_ALL,
    isSnapshot: Boolean = true
  ): ZStream[FdbTxn & FdbStream, Throwable, KeyValue] =
    ZStream.serviceWith[FdbStream](_.rangeNoRetry(start, end, limit, reverse, mode, isSnapshot)).flatten

  def runAsync[R, A](
    fn: => ZStream[FdbTxn & R, Throwable, A]
  )(implicit trace: Trace): ZStream[FdbStream & R, Throwable, A] =
    ZStream.serviceWithStream[FdbStream](_.runAsync[R, A](fn))
}

/**
 * Configuration for FoundationDB client connections.
 *
 * @param minConnections
 *   Minimum number of connections to maintain in the connection pool. Default: 1
 * @param maxConnections
 *   Maximum number of connections allowed in the connection pool. Default: 50
 * @param apiVersion
 *   FoundationDB API version to use (e.g., 710, 720). This should match your FoundationDB server version. Default: 710
 * @param clusterFile
 *   Path to the FoundationDB cluster file. If None, the default cluster file location will be used. Default: None
 * @param searchClusterFile
 *   Whether to search for the cluster file in default locations. Default: false
 * @param _retryPolicy
 *   Retry policy for failed operations. Format: "e<milliseconds>" for exponential backoff (e.g., "e10" = exponential starting at 10ms). Default: "e10"
 * @param timeToLive
 *   Time to live for database connections before they are recycled. Default: 30 seconds
 * @param performClassLoaderFixes
 *   Whether to perform classloader fixes for FoundationDB native library loading. This can help with classloader issues in some environments. Default: true
 * @param warnOnUnclosed
 *   Whether to warn when database connections are not properly closed. Useful for debugging resource leaks. Default: false
 * @param setFdbCLibraryPath
 *   Explicitly set the path to the FoundationDB C library (libfdb_c). If None, the library will be loaded from the default location. Default: None
 * @param getFdbCLibraryFromEnv
 *   Whether to get the FoundationDB C library path from environment variables. Default: true
 * @param fdbExecutor
 *   Optional custom executor for FoundationDB operations. If None, the default executor will be used. Default: None
 */
case class FoundationDbConfig(
  minConnections: Int,
  maxConnections: Int,
  apiVersion: Int,
  clusterFile: Option[String],
  searchClusterFile: Boolean,
  private val _retryPolicy: String,
  timeToLive: Duration,
  performClassLoaderFixes: Boolean,
  warnOnUnclosed: Boolean,
  setFdbCLibraryPath: Option[String],
  getFdbCLibraryFromEnv: Boolean,
  fdbExecutor: Option[Executor],
) {
  def withClusterFile(cf: Option[String]): FoundationDbConfig = copy(clusterFile = cf)

  lazy val retryPolicy: Schedule.WithState[Long, Any, Any, Duration] =
    _retryPolicy.headOption.flatMap {
      case 'e' =>
        _retryPolicy.tail.toIntOption.map { amount =>
          Schedule.exponential(amount.millis)
        }
      case _   => None
    }.getOrElse(FoundationDbConfig.defaultRetryPolicy)
}

object FoundationDbConfig {
  lazy val defaultRetryPolicy: Schedule.WithState[Long, Any, Any, Duration] = Schedule.exponential(10.millis)

  def default: FoundationDbConfig = FoundationDbConfig(
    minConnections = 1,
    maxConnections = 50,
    apiVersion = 710,
    clusterFile = None,
    searchClusterFile = false,
    _retryPolicy = "e10",
    timeToLive = 30.seconds,
    performClassLoaderFixes = true,
    warnOnUnclosed = false,
    setFdbCLibraryPath = None,
    getFdbCLibraryFromEnv = true,
    fdbExecutor = None,
  )

  final val MIN_CONNECTIONS            = "minconnections"
  final val MAX_CONNECTIONS            = "maxconnections"
  final val API_VERSION                = "apiversion"
  final val CLUSTER_FILE               = "clusterfile"
  final val SEARCH_CLUSTER_FILE        = "searchclusterfile"
  final val RETRY_POLICY               = "retrypolicy"
  final val TIME_TO_LIVE               = "timetolive"
  final val PERFORM_CLASS_LOADER_FIXES = "performclassloaderfixes"
  final val WARN_ON_UNCLOSED           = "warnonunclosed"
  final val SET_FDB_CLIBRARY_PATH      = "setfdbclibrarypath"
  final val GET_FDB_CLIBRARY_FROM_ENV  = "getfdbclibraryfromenv"

  def fromMap(map: Map[String, String]): FoundationDbConfig =
    map.toVector.foldLeft(default) { case (cfg, (key, value)) =>
      key.toLowerCase match {
        case `MIN_CONNECTIONS`            => value.toIntOption.fold(cfg)(value => cfg.copy(minConnections = value))
        case `MAX_CONNECTIONS`            => value.toIntOption.fold(cfg)(value => cfg.copy(maxConnections = value))
        case `API_VERSION`                => value.toIntOption.fold(cfg)(value => cfg.copy(apiVersion = value))
        case `CLUSTER_FILE`               => cfg.copy(clusterFile = Some(value))
        case `SEARCH_CLUSTER_FILE`        => value.toBooleanOption.fold(cfg)(value => cfg.copy(searchClusterFile = value))
        case `RETRY_POLICY`               => cfg.copy(_retryPolicy = value)
        case `TIME_TO_LIVE`               => value.toIntOption.fold(cfg)(value => cfg.copy(timeToLive = Duration(value, TimeUnit.SECONDS)))
        case `PERFORM_CLASS_LOADER_FIXES` => value.toBooleanOption.fold(cfg)(value => cfg.copy(performClassLoaderFixes = value))
        case `WARN_ON_UNCLOSED`           => value.toBooleanOption.fold(cfg)(value => cfg.copy(warnOnUnclosed = value))
        case `SET_FDB_CLIBRARY_PATH`      => cfg.copy(setFdbCLibraryPath = Some(value))
        case `GET_FDB_CLIBRARY_FROM_ENV`  => value.toBooleanOption.fold(cfg)(value => cfg.copy(getFdbCLibraryFromEnv = value))
//        case "fdbexecutor"  => value.toBooleanOption.fold(cfg)(value => cfg.copy(useZioBlockingExecutor = value))
        case _                            => cfg
      }
    }
}
