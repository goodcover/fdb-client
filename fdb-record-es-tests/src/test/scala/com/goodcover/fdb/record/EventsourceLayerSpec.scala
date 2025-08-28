package com.goodcover.fdb.record

import com.apple.foundationdb.record.metadata.{ Index, Key }
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper
import com.goodcover.fdb.*
import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabaseFactory
import com.goodcover.fdb.record.SharedTestLayers.simpleAppend
import com.goodcover.fdb.record.es.EventsourceLayer
import com.goodcover.fdb.record.es.EventsourceLayer.SnapshotSelectionCriteria
import com.goodcover.fdb.record.es.proto.{ PersistentRepr, Snapshot }
import com.google.protobuf.ByteString
import zio.stream.ZStream
import zio.test.*
import zio.{ Chunk, Clock, Random, Ref, Scope, ZIO, durationInt }

import java.util.concurrent.TimeUnit

object EventsourceLayerSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = (suite("EventsourceLayerSpec")(
    test("handle zero based numbers") {
      val elements = 5
      for {
        service      <- ZIO.service[EventsourceLayer]
        persistenceId = "pid"
        _            <- ZStream
                          .range(0, elements)
                          .mapZIO { i =>
                            simpleAppend(persistenceId, i.toLong, "tag1" :: Nil).as(i.toLong)
                          }
                          .runCollect
        consumer     <-
          service.currentEventsById(persistenceId, 0L, Long.MaxValue).runCollect
      } yield assertTrue(consumer.size == elements)
    },
    test("insert elements") {
      val totalElementsPerId     = 20
      val differentPersistentIds = 500
      val bytesPerMessage        = 2100
      for {
        _        <- ZIO.log("insert elements")
        service  <- ZIO.service[EventsourceLayer]
        body     <- Random.nextBytes(bytesPerMessage).map(bytes => ByteString.copyFrom(bytes.toArray))
        (dur, _) <- ZIO
                      .foreachParDiscard((0 until differentPersistentIds).toVector) { id =>
                        ZIO.foreachDiscard((0 until totalElementsPerId).toVector) { seqNr =>
                          Clock.currentTime(TimeUnit.MILLISECONDS).flatMap { ms =>
                            service.appendEvents(
                              PersistentRepr.of(
                                s"pid$id",
                                seqNr.toLong,
                                ms,
                                Seq("tag"),
                                body,
                                None,
                                None,
                                Map.empty
                              )
                            )
                          }
                        }
                      }
                      .withParallelism(32)
                      .timed

        _ <-
          ZIO.logInfo(
            s"Took ${dur.toMillis}ms to insert ${totalElementsPerId * differentPersistentIds} elements, which is ${(totalElementsPerId * differentPersistentIds) / dur.toSeconds}."
          )

        results               <- service.currentEventsById("pid1").runCollect
        _                     <- assertTrue(results.size == totalElementsPerId)
        _                     <- ZIO.logInfo("Starting stream")
        (dur, (bytes, count)) <- service
                                   .currentEventsByTag("tag")
                                   .runFold((0L, 0L)) { case ((bytes, count), ev) =>
                                     (bytes + ev.serializedSize, count + 1)
                                   }
                                   .timed
        _                     <-
          ZIO.logInfo(s"Took ${dur.toMillis}ms to read $count elements, which is ${Helpers.humanReadableSize(bytes, false)}.")

        _ <- assertTrue(count == totalElementsPerId * differentPersistentIds)
      } yield assertTrue(true)
    },
    test("tests have boundaries") {
      for {
        service <- ZIO.service[EventsourceLayer]
        results <- service.currentEventsById("pid1").runCollect
        _       <- assertTrue(results.isEmpty)
      } yield assertTrue(true)
    },
    test("follow elements") {
      for {
        _            <- ZIO.log("follow elements")
        service      <- ZIO.service[EventsourceLayer]
        persistenceId = "pid"
        ids          <- Ref.make(Chunk.empty[Long])
        consumer     <-
          service.eventsById(persistenceId, 0L, Long.MaxValue).tap(f => ids.update(_.appended(f.sequenceNr))).runCollect.fork
        seqNr        <- ZStream
                          .range(0, 10)
                          .mapZIO { i =>
                            simpleAppend(persistenceId, i.toLong, "tag1" :: Nil).as(i.toLong)
                          }
                          .runCollect
        _            <- ZIO.sleep(3.seconds)
        _            <- consumer.interrupt
        ids          <- ids.get
      } yield assertTrue(ids == seqNr)
    },
    test("follow elements realistic") {
      for {
        service      <- ZIO.service[EventsourceLayer]
        persistenceId = "pid"
        ids          <- Ref.make(Chunk.empty[Long])
        seqNr1       <- ZStream
                          .range(0, 5)
                          .mapZIO { i =>
                            simpleAppend(persistenceId, i.toLong, "tag1" :: Nil).as(i.toLong)
                          }
                          .runCollect
        consumer     <-
          service.eventsById(persistenceId, 0L, Long.MaxValue).tap(f => ids.update(_.appended(f.sequenceNr))).runCollect.fork
        _            <- ZIO.sleep(500.millis)
        seqNr2       <- ZStream
                          .range(5, 10)
                          .mapZIO { i =>
                            simpleAppend(persistenceId, i.toLong, "tag1" :: Nil).as(i.toLong)
                          }
                          .runCollect <* ZIO.sleep(500.millis)
        _            <- consumer.interrupt
        ids          <- ids.get
      } yield assertTrue(ids == seqNr1 ++ seqNr2)
    },
    test("follow tag") {
      for {
        _            <- ZIO.log("follow tag")
        service      <- ZIO.service[EventsourceLayer]
        persistenceId = "pid"
        seenRecords  <- Ref.make(Chunk.empty[(String, Long)])
        consumer     <-
          service
            .eventsByTag("tag1", None)
            .tap(f => seenRecords.update(_.appended((f.persistenceId, f.sequenceNr))))
            .runCollect
            .fork
        seqNr        <- ZStream
                          .range(0, 20)
                          .mapZIO { i =>
                            val tagName = if (i < 10) "tag1" else "tag2"
                            simpleAppend(persistenceId, i.toLong, tagName :: Nil).flatMap { pr =>
                              ZIO.sleep(200.millis).as((tagName, pr.persistenceId, pr.sequenceNr))
                            }
                          }
                          .runCollect

        tag1Uuids      = seqNr.filter(_._1 == "tag1")
        _             <- consumer.interrupt
        uuids         <- seenRecords.get
        tag1UuidsPost <- service.currentEventsByTag("tag1").map(f => (f.persistenceId, f.sequenceNr)).runCollect
      } yield assertTrue( //
        uuids.size == tag1Uuids.size,
        uuids == tag1UuidsPost,
        uuids == tag1Uuids.map(p => (p._2, p._3)),
      )
    },
    test("get primary splits") {
      for {
        service <- ZIO.service[EventsourceLayer]
        payload <- Random.nextBytes(SplitHelper.SPLIT_RECORD_SIZE * 20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))
        _       <- ZStream
                     .range(0, 100)
                     .mapZIOPar(5) { i =>
                       val seqNr = i % 10
                       val id    = i / 10
                       simpleAppend(
                         s"pid-$id",
                         seqNr.toLong,
                         "tag1" :: Nil,
                         payload = payload,
                       )
                     }
                     .runCollect
        _       <- service.getPrimarySplits.runCollect
        _       <- service.getTagSplits.runCollect

      } yield assertTrue( //
        true
      )
    },
    test("save snapshots") {
      val max = 100
      for {
        service <- ZIO.service[EventsourceLayer]
        payload <- Random.nextBytes(SplitHelper.SPLIT_RECORD_SIZE * 20 + 1).map(bytes => ByteString.copyFrom(bytes.toArray))

        maxSeq   <- ZStream
                      .range(0, max)
                      .mapZIOPar(5) { _ =>
                        Random
                          .nextLongBounded(max.toLong * 1000)
                          .tap { int =>
                            service.saveSnapshot(Snapshot.of("pid1", int, max * 1000 - int, payload, None, None, Map.empty), 3)
                          }
                      }
                      .runCollect
        resultEv <- service.selectLatestSnapshot("pid1", SnapshotSelectionCriteria()).runCollect
        _        <- assertTrue(maxSeq.max == resultEv.head.sequenceNr)
        maxSeq   <- ZIO.succeed(maxSeq.sorted.take(4)(3))
        resultEv <- service.selectLatestSnapshot("pid1", SnapshotSelectionCriteria(maxSequenceNr = maxSeq + 1)).runCollect
        _        <- assertTrue(maxSeq == resultEv.head.sequenceNr)

      } yield assertTrue( //
        true
      )
    },
    test("rebuild indexes") {
      val max = 100
      for {
        service        <- ZIO.service[EventsourceLayer]
        _              <- ZStream
                            .range(0, max)
                            .mapZIOPar(5) { i =>
                              simpleAppend("pid1", i.toLong, Seq.empty)
                            }
                            .runCollect
        _              <- service.unsafeAlterMeta { rmds =>
                            ZIO
                              .fromCompletableFuture(
                                rmds.addIndexAsync("PersistentRepr", new Index("twSeqNr", Key.Expressions.field("sequenceNr")))
                              )
                              .unit
                          }
        rebuiltIndexes <- service.rebuildDefaultIndexes

      } yield assertTrue( //
        rebuiltIndexes.isEmpty
      )
    },
    test("highest sequenceNr") {
      val max = 100
      for {
        service      <- ZIO.service[EventsourceLayer]
        _            <- service.highestSequenceNr("pid1", 0L).map(result => assertTrue(result == 0L))
        _            <- ZStream
                          .range(0, max)
                          .mapZIO { i =>
                            simpleAppend("pid1", i.toLong, Seq.empty)
                          }
                          .runCollect
        _            <- service.deleteTo("pid1", max.toLong - 2)
        highestSeqNr <- service.highestSequenceNr("pid1", 0L)

      } yield assertTrue( //
        highestSeqNr == max - 1
      )
    },
    test("middle deletions") {
      val max       = 100
      val lastSeqNr = max - 1
      for {
        // Forced even to make sure the test makes sense, otherwise brain hurts
        _       <- assertTrue((max / 2) + (max / 2) == max)
        service <- ZIO.service[EventsourceLayer]
        _       <- ZStream
                     .range(0, max)
                     .mapZIO { i =>
                       simpleAppend("pid1", i, Seq.empty)
                     }
                     .runCollect
        _       <- service.deleteFromTo("pid1", max.toLong / 2, lastSeqNr - 2)
        _       <- service.deleteFromTo("pid1", 1, 1)
        count   <- service.currentEventsById("pid1", 0L, Long.MaxValue).map(_.sequenceNr).runCollect

      } yield assertTrue(              //
        count.size == max / 2 + 2 - 1, // -1 because of the single delete above
        count.last == lastSeqNr,
        count.dropRight(1).last == lastSeqNr - 1
      )
    },
    test("select snapshot sequenceNr") {
      val max = 10
      for {
        service <- ZIO.service[EventsourceLayer]
        _       <- ZStream
                     .range(0, max)
                     .mapZIO { i =>
                       service.saveSnapshot(Snapshot.of("pid1", i.toLong, 1L, ByteString.EMPTY, None, None, Map.empty))
                     }
                     .runCollect
        ss1     <- service.selectLatestSnapshot("pid1", SnapshotSelectionCriteria(maxSequenceNr = max.toLong)).runCollect
        ss2     <- service.selectLatestSnapshot("pid1", SnapshotSelectionCriteria()).runCollect
        _       <- service.deleteSnapshot("pid1", SnapshotSelectionCriteria(maxSequenceNr = (max / 2).toLong))
        ss2a    <- service.selectLatestSnapshot("pid1", SnapshotSelectionCriteria(maxSequenceNr = (max / 2).toLong)).runCollect
        ss2b    <- service.selectLatestSnapshot("pid1", SnapshotSelectionCriteria()).runCollect

      } yield assertTrue( //
        ss1 == ss2,
        ss2a.isEmpty,
        ss2b.nonEmpty,
      )
    },
    test("save/load metadata") {
      for {
        service <- ZIO.service[EventsourceLayer]
        lmPre   <- service.loadMetadata
        _       <- service.saveMetadata
        lm      <- service.loadMetadata

      } yield assertTrue( //
        lmPre.isEmpty,
        lm.nonEmpty,
      )
    }
  ) @@ TestAspect.withLiveClock)
    .provideSome[FdbRecordDatabaseFactory](
      TestId.layer >+>
        (SharedTestLayers.ConfigLayer >>> EventsourceLayer.live) >+> SharedTestLayers.ClearAll
    )
    .provideShared(
      FdbSpecLayers.sharedLayer >+> FdbRecordDatabaseFactory.live
    )

}
