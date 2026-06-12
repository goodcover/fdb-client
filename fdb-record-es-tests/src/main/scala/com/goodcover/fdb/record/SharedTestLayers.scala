package com.goodcover.fdb.record

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory
import com.goodcover.fdb.{ BuildInfo, TestId }
import com.goodcover.fdb.record.es.EventsourceLayer.EventsourceConfig
import com.goodcover.fdb.record.es.EventsourceLayer
import com.goodcover.fdb.record.es.proto.PersistentRepr
import com.google.protobuf.ByteString
import zio.{ Clock, Unsafe, ZIO, ZLayer }

import java.util.concurrent.TimeUnit

object SharedTestLayers {

  def ConfigLayer(implicit fn: sourcecode.FullName): ZLayer[TestId, Nothing, EventsourceConfig] = ZLayer.scoped {
    for {
      testKeySpace   <- ZIO.service[TestId]
      id              = testKeySpace.id
      extraCrossToken = sys.props.get("cross-token").getOrElse("no-xtoken")
      pathPerTest     = s"${fn.value}:$id:${BuildInfo.scalaVersion}:$extraCrossToken"
      directory       = new KeySpaceDirectory(s"tests", KeySpaceDirectory.KeyType.STRING, pathPerTest)
      _              <- ZIO.logInfo(s"provisioning the following path '$pathPerTest' in keyspaceDirectory '$directory''")
    } yield EventsourceConfig.makeDefaultConfig(directory)
  }

  /**
   * Deletes all records when the suite scope opens and again when it closes.
   * The pre-clean matters because the keyspace path is deterministic per spec:
   * a run that dies without running finalizers (CI timeout, kill -9) leaves
   * data behind that would poison the next run.
   */
  lazy val ClearAll: ZLayer[EventsourceLayer, Nothing, Any] = ZLayer.scoped {
    for {
      es <- ZIO.service[EventsourceLayer]
      _  <- Unsafe.unsafe { implicit unsafe =>
              es.unsafeDeleteAllRecords.orDie *>
                ZIO.addFinalizer(es.unsafeDeleteAllRecords.orDie)
            }
    } yield ()
  }

  def simpleAppend(
    id: String,
    seqNr: Long,
    tags: Seq[String],
    payload: ByteString = ByteString.EMPTY,
  ): ZIO[EventsourceLayer, Throwable, PersistentRepr] =
    ZIO.serviceWithZIO[EventsourceLayer] { service =>
      Clock.currentTime(TimeUnit.MILLISECONDS).flatMap { ms =>
        val pr = PersistentRepr.of(
          id,
          seqNr,
          ms,
          tags,
          payload,
          None,
          None,
          Map.empty
        )
        service.appendEvents(pr).as(pr)
      }
    }

}
