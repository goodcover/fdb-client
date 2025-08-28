package com.goodcover.fdb.es.pekko

import com.goodcover.fdb.record.RecordDatabase.{ FdbRecordDatabase, FdbRecordDatabaseFactory }
import com.goodcover.fdb.record.es as ES
import com.goodcover.fdb.{ FdbDatabase, FdbPool, FoundationDbConfig }
import com.typesafe.config.Config
import org.apache.pekko.actor.{ ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import zio.{ Exit, Scope, Unsafe, ZLayer }

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters.CollectionHasAsScala

object FdbEsExtension extends ExtensionId[FdbEsExtensionImpl] with ExtensionIdProvider {
  override def lookup: FdbEsExtension.type                  = FdbEsExtension
  override def createExtension(system: ExtendedActorSystem) = new FdbEsExtensionImpl(system)
}

class FdbEsExtensionImpl(system: ExtendedActorSystem) extends Extension {
  // Potentially provide a Runtime here?
  private val _runtime: zio.Runtime[Any] = zio.Runtime.default

  private val sessions = new ConcurrentHashMap[String, (zio.Runtime[FdbEventJournalConnector.R], Scope.Closeable)]()

  def sharedPath(path: String): String = path
    .replaceAll("\\.snapshot-store$", "")
    .replaceAll("\\.journal$", "")
    .replaceAll("\\.query$", "")

  private def createSession(sharedPath: String) = {
    val subConfig      = system.settings.config.getConfig(sharedPath)
    val classHelper    = system.dynamicAccess
    val configProvider = classHelper
      .createInstanceFor[FdbPekkoConfigProvider](subConfig.getString("configProvider"), Seq(classOf[Config] -> subConfig))
      .get

    val fdbConfigGetter = subConfig.entrySet().asScala
    val stringMap       = fdbConfigGetter
      .map(pair => pair.getKey -> pair.getValue.unwrapped().toString)
      .toMap

    val clusterConfig = FoundationDbConfig.fromMap(stringMap)

    val layer = ZLayer.succeed(clusterConfig) ++
      configProvider.get() >+>
      FdbPool.make() >+>
      FdbDatabase.layer >+>
      FdbRecordDatabaseFactory.live >+>
      FdbRecordDatabase.live >+>
      ES.EventsourceLayer.live

    Unsafe.unsafe { implicit u =>
      val (result, scope) = _runtime.unsafe.run {
        Scope.make.flatMap { scope =>
          layer.toRuntime.provide(ZLayer.succeed(scope)).map(r => (r, scope))
        }
      }
        .getOrThrowFiberFailure()

      (result, scope)
    }
  }

  def runtime(path: String): zio.Runtime[FdbEventJournalConnector.R] = {
    val parentPath = sharedPath(path)
    sessions.computeIfAbsent(parentPath, _ => createSession(parentPath))._1
  }

  def snapshotFilter(sharedPath: String): Option[SnapshotFilter] = {
    val subConfig   = system.settings.config.getConfig(sharedPath)
    val classHelper = system.dynamicAccess

    if (subConfig.getIsNull("snapshotFilter")) None
    else
      classHelper.createInstanceFor[SnapshotFilter](subConfig.getString("snapshotFilter"), Seq.empty).toOption
  }

  private def close(executionContext: ExecutionContext) = Unsafe.unsafe { implicit u =>
    implicit val ec: ExecutionContext = executionContext
    val closing                       = sessions.values().asScala.map { case (runtime, scope) =>
      runtime.unsafe.runToFuture(scope.close(Exit.Success(()))): Future[Unit]
    }
    Future.sequence(closing)
  }

  system.whenTerminated.foreach(_ => close(ExecutionContext.global))(ExecutionContext.global)

}
