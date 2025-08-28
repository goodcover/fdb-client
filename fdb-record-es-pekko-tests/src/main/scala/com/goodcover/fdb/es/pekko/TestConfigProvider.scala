package com.goodcover.fdb.es.pekko

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType
import com.goodcover.fdb.{ BuildInfo, TestFdbLogger }
import com.goodcover.fdb.record.es.EventsourceLayer.EventsourceConfig
import com.typesafe.config.{ Config, ConfigFactory }
import zio.{ Trace, ZLayer }

class TestConfigProvider(c: Config) extends FdbPekkoConfigProvider {

  override def get(): ZLayer[Any, Throwable, EventsourceConfig] = {
    val name   = c.getString("testName")
    val parent = new KeySpaceDirectory("TCK", KeyType.STRING, name)
    zio.Runtime.removeDefaultLoggers ++
      zio.Runtime.addLogger(TestFdbLogger) >+>
      ZLayer.succeed(
        EventsourceConfig.makeDefaultConfig(parent)
      )
  }

}

object TestConfigProvider {
  private[pekko] def config(implicit trace: Trace) = ConfigFactory
    .parseString( //
      s"""
         |testName = "$trace:${BuildInfo.scalaVersion}"
         |pekko {
         |  persistence {
         |    journal.plugin = "fdb.journal"
         |    snapshot-store.plugin = "fdb.snapshot-store"
         |  }
         |  test {
         |    timefactor = $${?PEKKO_TEST_TIMEFACTOR}
         |    single-expect-default = 20s
         |    filter-leeway = 20s
         |  }
         |}
         |
         |fdb {
         |  configProvider = "com.goodcover.fdb.es.pekko.TestConfigProvider"
         |  testName = "$trace:${BuildInfo.scalaVersion}"
         |  journal {
         |  }
         |  snapshot-store {
         |  }
         |  query {
         |  }
         |}
         |""".stripMargin
    )
    .resolve()
}
