package com.goodcover.fdb.record

import com.apple.foundationdb.record.RecordMetaData
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory
import com.goodcover.fdb.record.RecordDatabase.{ FdbMetadata, FdbRecordDatabaseFactory }
import com.goodcover.fdb.{ BuildInfo, TestId }
import com.google.protobuf.Descriptors.FileDescriptor
import zio.{ Unsafe, ZIO, ZLayer }

/**
 * Suite-scoped layers for record-store tests: a unique keyspace per test
 * (derived from the spec name, [[TestId]], Scala version and cross token), a
 * [[BaseLayer]] on top of it, and cleanup on scope close.
 */
object RecordTestLayers {

  /**
   * A [[RecordConfig]] rooted at a keyspace unique to the calling spec, in the
   * same shape as `SharedTestLayers.ConfigLayer` for the eventsource side.
   */
  def configLayer(
    metadata: => RecordMetaData,
    descriptor: FileDescriptor,
    persistLocalMetadata: Boolean = true,
  )(implicit fn: sourcecode.FullName): ZLayer[TestId, Nothing, RecordConfig] = ZLayer {
    for {
      testId         <- ZIO.service[TestId]
      extraCrossToken = sys.props.get("cross-token").getOrElse("no-xtoken")
      pathPerTest     = s"${fn.value}:${testId.id}:${BuildInfo.scalaVersion}:$extraCrossToken"
      directory       = new KeySpaceDirectory("tests", KeySpaceDirectory.KeyType.STRING, pathPerTest)
      _              <- ZIO.logDebug(s"provisioning the following path '$pathPerTest' in keyspaceDirectory '$directory'")
      ks              = RecordKeySpace.makeDefaultConfig(directory)
    } yield RecordConfig(
      recordKeySpace = ks,
      localMetadata = FdbMetadata(metadata, ks.tablePath),
      localFileDescriptor = descriptor,
      persistLocalMetadata = persistLocalMetadata,
    )
  }

  /**
   * A [[BaseLayer]] from a factory-provided database and a [[RecordConfig]].
   */
  val baseLayer: ZLayer[FdbRecordDatabaseFactory & RecordConfig, Nothing, BaseLayer] = ZLayer {
    for {
      factory  <- ZIO.service[FdbRecordDatabaseFactory]
      cfg      <- ZIO.service[RecordConfig]
      recordDb <- factory.db.orDie
      base     <- BaseLayer.make(recordDb, cfg)
    } yield base
  }

  /**
   * Deletes the store and its metadata when the suite scope opens and again
   * when it closes. The pre-clean matters because the keyspace path is
   * deterministic per spec: a run that dies without running finalizers (CI
   * timeout, kill -9) leaves data behind that would poison the next run.
   */
  val clearAll: ZLayer[BaseLayer, Nothing, Unit] = ZLayer.scoped {
    for {
      base <- ZIO.service[BaseLayer]
      _    <- Unsafe.unsafe { implicit unsafe =>
                base.unsafeDeleteAllRecords.orDie *>
                  ZIO.addFinalizer(base.unsafeDeleteAllRecords.orDie)
              }
    } yield ()
  }

  /** Use [[clearAll]]: it also pre-cleans leftovers from killed runs. */
  @deprecated("use clearAll, which also cleans up leftovers from killed runs", "0.5.3")
  val clearAllOnClose: ZLayer[BaseLayer, Nothing, Unit] = clearAll
}
