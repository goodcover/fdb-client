package com.goodcover.fdb.record

import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabaseFactory
import com.goodcover.fdb.record.SharedTestLayers.simpleAppend
import com.goodcover.fdb.record.es.EventsourceLayer
import com.goodcover.fdb.{ FdbSpecLayers, TestId }
import zio.*
import zio.test.*

object InterruptSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = (suite("InterruptSpec")(
    test("interrupt") {
      val pId = "peristenceId"
      for {
        service <- ZIO.service[EventsourceLayer]
        _       <- simpleAppend(pId, 1L, "tag1" :: Nil)
        num     <- Ref.make(0)
        _       <-
          service.eventsById(pId, 0L, Long.MaxValue).runForeachChunk(chk => num.update(_ + chk.size)).fork
        _       <- ZIO.sleep(1.second)

      } yield assertTrue(true)

    }
  ) @@ TestAspect.withLiveClock)
    .provideSome[FdbRecordDatabaseFactory](
      TestId.layer >+>
        (SharedTestLayers.ConfigLayer >+> EventsourceLayer.live) >+> SharedTestLayers.ClearAll
    )
    .provideShared(
      FdbSpecLayers.sharedLayer >+> FdbRecordDatabaseFactory.live
    )

}
