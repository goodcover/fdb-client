package org.apache.spark.sql.fdb

import com.apple.foundationdb.record.metadata.expressions.{ FieldKeyExpression, KeyExpression, ThenKeyExpression }
import com.goodcover.fdb.record.RecordDatabase
import com.goodcover.fdb.record.RecordDatabase.{ FdbRecordDatabase, FdbRecordDatabaseFactory }
import com.goodcover.fdb.{ FdbDatabase, FdbPool, FoundationDbConfig, LayerProvider }
import nl.vroste.rezilience.RateLimiter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.fdb.FdbTable.MetadataWith
import org.apache.spark.sql.fdb.StorageHelper.sessionCache
import org.apache.spark.sql.protobuf.functions.*
import org.apache.spark.sql.proto.{ ProtobufOptions, SchemaConverters }
import org.apache.spark.sql.types.StructType
import zio.{ Runtime, Scope, Trace, Unsafe, ZIO, ZLayer, durationInt }

import java.io.IOException
import scala.concurrent.blocking
import scala.jdk.CollectionConverters.CollectionHasAsScala

class StorageHelper private (cfg: SparkFdbConfig) {
  private implicit val unsafe: Unsafe = Unsafe.unsafe(u => u)

  // Layering from ZIO -> Spark, aka ZIO[..A] -> A
  type R = FdbPool & FdbDatabase & FdbRecordDatabase & Scope
  private val runtimeLayer: LayerProvider = cfg.layerProvider

  protected def mkLayers: ZLayer[
    Any,
    Throwable,
    Any & FoundationDbConfig & FdbPool & FdbDatabase & FdbRecordDatabaseFactory & FdbRecordDatabase & Scope
  ] = runtimeLayer.get ++
    ZLayer.succeed(cfg.dbConfig) >+>
    FdbPool.make() >+>
    FdbDatabase.layer >+>
    FdbRecordDatabaseFactory.live >+>
    FdbRecordDatabase.live >+>
    Scope.default

  /**
   * Stolen from the [[Runtime.unsafe.fromLayer(mkLayers)]] but made blocking
   */
  val layers: Runtime.Scoped[R] = LayerProvider.mkLayerBlocking(mkLayers)

  val db: FdbRecordDatabase = layers.environment.get[FdbRecordDatabase]

  val scope: Scope = layers.environment.get[Scope]

  def unsafeRunBlocking[R0 <: R, A](zio: => ZIO[R, Throwable, A])(implicit trace: Trace): A =
    blocking(layers.unsafe.run(ZIO.blocking(zio)).getOrThrow())

  def unsafeRun[R0 <: R, A](zio: => ZIO[R, Throwable, A])(implicit trace: Trace): A =
    layers.unsafe.run(zio).getOrThrow()

  def metaDataToColumns(cfg: MetadataWith[ReadConf]): StructType = {
    val recordType = cfg.recordMetaData.recordType(cfg.config.recordType)
    val st         = SchemaConverters.toSchema(
      recordType.getDescriptor,
      ProtobufOptions(
        Map( //
          // This actually doesn't do anything with how we use it, but for refactoring
          // sake ill leave it here
          "emit.default.values" -> true.toString
        )
      )
    )
    st
  }

  /** This function does not work properly yet */
  def getIndexes(cfg: MetadataWith[ReadConf]): Seq[String] = {
    val recordType = cfg.recordMetaData.recordType(cfg.config.recordType)

    val primaryKeyList = recordType.getPrimaryKey

    def processKey(ke: KeyExpression, fieldNames: Vector[String]): Vector[String] =
      ke match {
        case fk: FieldKeyExpression =>
          fieldNames :+ fk.getFieldName
        case tk: ThenKeyExpression  =>
          val children = tk.getChildren.asScala.toList
          children.foldLeft(fieldNames) { (fieldNames, child) =>
            processKey(child, fieldNames)
          }
      }

    val pks = processKey(primaryKeyList, Vector.empty)
    pks
  }

  def metaData(tableConfig: ReadConf)(implicit trace: Trace): Option[RecordDatabase.FdbMetadata] = unsafeRunBlocking {
    db.runAsync {
      for {
        _      <- ZIO.logInfo(s"Loading the metadata from the path ${tableConfig.metaKeySpacePath}")
        result <- db.loadMetadata(
                    tableConfig.metaKeySpacePath,
                    Option(tableConfig.keyspaceProvider.fileDescriptor()),
                    tableConfig.tableKeySpacePath,
                  )
        opt    <- result match {
                    case a @ Some(value) =>
                      ZIO
                        .logInfo(
                          s"Found metadata at ${tableConfig.metaKeySpacePath},tablepath=${tableConfig.tableKeySpacePath},version=${value.metadata.getVersion}"
                        )
                        .as(a)

                    case a @ None => ZIO.logInfo(s"Did not find metadata at ${tableConfig.metaKeySpacePath}").as(a)

                  }
      } yield opt
    }
  }

  def unsafeMetaData(tableConfig: ReadConf): RecordDatabase.FdbMetadata =
    metaData(tableConfig) match {
      case Some(value) => value
      case None        =>
        throw new IllegalStateException(
          s"Missing metadata at the location ${tableConfig.metaKeySpacePath}, you'll need to manually initialize it outside of spark"
        )
    }

  private[fdb] def makeRateLimiter(tableConfig: ReadConf, totalPartitions: Int) =
    tableConfig.maxRequestsPerSecond.fold(FdbWriter.noOp) { limit =>
      val minLimit = Math.max(limit / totalPartitions, 1)
      unsafeRun(RateLimiter.make(minLimit, 1.second).provideSomeEnvironment(_.add(scope)))
    }

  def shutdown(): Unit =
    sessionCache.release(this, cfg.releaseDelayMs)

  private def _shutdown(): Unit =
    layers.unsafe.shutdown()
}

object StorageHelper extends Logging {

  /** Activate a cached method for doing this */
  def apply(conf: SparkFdbConfig): StorageHelper = sessionCache.acquire(conf)

  private[fdb] val sessionCache =
    new RefCountedCache[SparkFdbConfig, StorageHelper](createSession, destroySession)

  private def createSession(conf: SparkFdbConfig): StorageHelper = {
    val helper = new StorageHelper(conf)
    logDebug(s"Attempting to open native connection to Fdb with ${conf.dbConfig}")
    try {
      logInfo(s"Connected to Fdb cluster.")
      helper
    } catch {
      case e: Throwable =>
        throw new IOException(s"Failed to open native connection to Fdb at ${conf.dbConfig} :: ${e.getLocalizedMessage}", e)
    }
  }

  private def destroySession(session: StorageHelper): Unit = {
    session._shutdown()
    logInfo(s"Disconnected from Fdb cluster.")
  }
}
