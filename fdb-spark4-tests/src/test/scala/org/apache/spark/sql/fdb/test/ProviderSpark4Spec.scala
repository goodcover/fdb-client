package org.apache.spark.sql.fdb.test

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType
import com.apple.foundationdb.record.provider.foundationdb.keyspace.{ KeySpace, KeySpaceDirectory, KeySpacePath }
import com.goodcover.fdb.LayerProvider
import com.goodcover.fdb.record.es.EventsourceMeta
import com.google.protobuf.Descriptors
import org.apache.spark.sql.fdb.ReadConf
import org.apache.spark.sql.fdb.ReadConf.KeySpaceProvider
import zio.test.*
import zio.{ Scope, ZLayer }

import scala.annotation.unused

object ProviderSpark4Spec extends ZIOSpecDefault {

  class Foo(@unused map: Map[String, String]) extends KeySpaceProvider with LayerProvider {
    private def hierarchy =
      new KeySpaceDirectory("eventsource", KeyType.LONG, 5001)
        .addSubdirectory(new KeySpaceDirectory("table", KeyType.STRING, "t"))
        .addSubdirectory(new KeySpaceDirectory("meta", KeyType.STRING, "m"))

    override def keySpace: KeySpace = new KeySpace(hierarchy)

    override def tablePath: KeySpacePath = keySpace.path("eventsource").add("table")

    override def metaPath: KeySpacePath = keySpace.path("eventsource").add("meta")

    override def fileDescriptor(): Descriptors.FileDescriptor = EventsourceMeta.descriptor

    override def get: ZLayer[Any, Nothing, Any] = ZLayer.empty
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("ProviderSpec")(
    test("test providers") {
      val cfg = ReadConf(
        "",
        Some(getClass.getName + "Foo"),
        None,
        None,
        None,
        None,
        Map.empty
      )

      assertTrue(cfg.keyspaceProvider.keySpace.getRoot.getName == "/")
    }
  )
}
