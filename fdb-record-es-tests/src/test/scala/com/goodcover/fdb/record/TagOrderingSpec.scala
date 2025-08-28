package com.goodcover.fdb.record

import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabaseFactory
import com.goodcover.fdb.record.es.EventsourceLayer
import com.goodcover.fdb.record.es.proto.PersistentRepr
import com.goodcover.fdb.{ FdbSpecLayers, TestId }
import zio.*
import zio.test.*

object TagOrderingSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = (suite("TagOrderingSpec")(
    test("filter works") {

      val tags = Seq("foo")
      // ts, pid, seqNr
      val list = Seq(
        (1, "a", 1),
        (1, "b", 1),
        (1, "c", 1),
        (2, "b", 2),
        (2, "b", 3),
        (3, "c", 2), // <-- this and later should come through
        (4, "a", 2),
        (5, "a", 3),
        (6, "b", 4),
        (6, "b", 5),
        (7, "a", 4),
      )
      for {
        service <- ZIO.service[EventsourceLayer]
        _       <- ZIO.foreachDiscard(list) { case (ts, pid, seqNr) =>
                     val event = PersistentRepr(
                       persistenceId = pid,
                       sequenceNr = seqNr.toLong,
                       timestamp = ts.toLong,
                       tags = tags,
                     )
                     service.appendEvents(event)
                   }
        result  <-
          service
            .currentEventsByTag(tags.head, Some(EventsourceLayer.TagOffset(3L, "c", 1L)))
            .runCollect

        count <- service.totalRecordCount

      } yield assertTrue(
        result.nonEmpty,
        result.length == 6,
        count.toInt == list.length
      )

    },
    test("prefixes don't match") {

      // ts, pid, seqNr
      val list = Seq(
        (1, "a", 1, "tag1"),
        (1, "b", 1, "tag1"),
        (1, "a", 2, "tag1"),
        (2, "b", 2, "tag1"),
        (2, "b", 3, "tag1"), // <-- here
        (3, "c", 2, "tag12"),
        (4, "c", 3, "tag11"),
        (4, "a", 3, "tag1"),
        (5, "a", 4, "tag1"),
        (6, "b", 4, "tag1"),
        (6, "b", 5, "tag1"),
        (7, "a", 5, "tag1"),
      )
      for {
        service <- ZIO.service[EventsourceLayer]
        _       <- ZIO.foreachDiscard(list) { case (ts, pid, seqNr, tag) =>
                     val event = PersistentRepr(
                       persistenceId = pid,
                       sequenceNr = seqNr,
                       timestamp = ts.toLong,
                       tags = tag :: Nil,
                     )
                     service.appendEvents(event)
                   }
        result  <-
          service
            .currentEventsByTag("tag1", Some(EventsourceLayer.TagOffset(2L, "b", 2L)))
            .runCollect

        count <- service.totalRecordCount

      } yield assertTrue(
        result.nonEmpty,
        result.length == 6,
        count.toInt == list.length
      )

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
