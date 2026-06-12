package com.goodcover.fdb.lucene

import com.apple.foundationdb.record.provider.foundationdb.properties.{ RecordLayerPropertyKey, RecordLayerPropertyStorage }
import com.goodcover.fdb.record.RecordDatabase.{ FdbRecordDatabaseFactory, RecordContextOptions }
import com.goodcover.fdb.record.{ BaseLayer, RecordTestLayers }
import com.goodcover.fdb.{ FdbSpecLayers, TestId }
import zio.*
import zio.test.*

import java.util.concurrent.ForkJoinPool

/**
 * Proves the database-level plumbing: a [[RecordContextOptions]] set on the
 * factory reaches every transaction opened through `runAsync`, and record-layer
 * async work runs on the owned executor instead of the JDK common pool.
 */
object RecordContextOptionsSpec extends ZIOSpecDefault {

  private val testProp = RecordLayerPropertyKey.stringPropertyKey("goodcover_test_prop", "default")

  private val options = RecordContextOptions.default.withProperties(
    RecordLayerPropertyStorage.newBuilder().addProp(testProp, "configured").build()
  )

  override def spec: Spec[TestEnvironment with Scope, Any] = (suite("RecordContextOptionsSpec")(
    test("database-level properties reach every transaction") {
      ZIO.serviceWithZIO[BaseLayer] { layer =>
        layer.withStoreTxn { store =>
          ZIO.succeed(store.underlyingStore.getContext.getPropertyStorage.getPropertyValue(testProp))
        }
      }.map(value => assertTrue(value == "configured"))
    },
    test("record-layer async work does not run on the JDK common pool") {
      ZIO.serviceWithZIO[BaseLayer] { layer =>
        layer.withStoreTxn { store =>
          ZIO.succeed(store.underlyingStore.getExecutor)
        }
      }.map(executor => assertTrue(executor ne ForkJoinPool.commonPool()))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(2.minutes))
    .provideSome[FdbRecordDatabaseFactory](
      TestId.layer >+>
        RecordTestLayers.configLayer(LuceneMetadata.build(), LuceneMetadata.descriptor) >+>
        RecordTestLayers.baseLayer >+>
        RecordTestLayers.clearAll
    )
    .provideShared(
      FdbSpecLayers.sharedLayer >+> FdbRecordDatabaseFactory.live(options)
    )
}
