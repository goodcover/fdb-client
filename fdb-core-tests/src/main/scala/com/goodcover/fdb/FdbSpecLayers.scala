package com.goodcover.fdb

import zio.test.SuiteId
import zio.{ LogLevel, Runtime, Trace, ZIO, ZLayer }

object FdbSpecLayers {

  private[fdb] type TEnv = FdbDatabase & FdbStream & FdbPool & FoundationDbConfig

  private val Logger = TestFdbLogger.filterLogLevel(_ >= LogLevel.Info)

  /** Basic layer with nothing really changed, it's shared. */
  lazy val sharedLayer: ZLayer[Any, Throwable, FdbDatabase & FdbStream & FdbPool & FoundationDbConfig] =
    sharedLayerBase ++
      Runtime.removeDefaultLoggers ++
      Runtime.addLogger(Logger)

  private def sharedLayerBase(implicit
    trace: Trace
  ): ZLayer[Any, Throwable, FdbPool & FdbDatabase & FdbStream & FoundationDbConfig] =
    ((ZLayer.succeed(
      FoundationDbConfig.default.copy(getFdbCLibraryFromEnv = true, searchClusterFile = true)
    ) >+> FdbPool.make()) >+> FdbDatabase.layer) >+> FdbStream.layer >+> reportOnCleanup

  private lazy val reportOnCleanup: ZLayer[FdbDatabase, Nothing, Any] = ZLayer.scoped {
    ZIO.serviceWithZIO[FdbDatabase](db =>
      ZIO.addFinalizer(db.getOpenTransactions.flatMap(open => ZIO.logInfo(s"Open transactions = $open")))
    )
  }

  /** Refer to [[suiteSubspaceLayer]] */
  private def suiteSubspace(implicit t: Trace): ZIO[FdbDatabase, Throwable, SuiteSubspace] = SuiteId.newRandom.flatMap { id =>
    ZIO
      .serviceWithZIO[FdbDatabase](_.directoryCreateOrOpen(t.toString :: s"test$t:${BuildInfo.scalaVersion}:$id" :: Nil))
      .map(SuiteSubspace(_, id))
  }

  /**
   * Create a subspace for a test suiteId, these are project centric, we use
   * trace to uniquely name them per spec
   */
  def suiteSubspaceLayer(implicit t: Trace): ZLayer[FdbDatabase, Throwable, SuiteSubspace] =
    ZLayer.fromZIO(suiteSubspace)

}
