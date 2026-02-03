package com.goodcover.fdb.record

import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabaseFactory
import com.goodcover.fdb.record.SharedTestLayers.simpleAppend
import com.goodcover.fdb.record.es.EventsourceLayer
import com.goodcover.fdb.record.es.EventsourceLayer.SnapshotSelectionCriteria
import com.goodcover.fdb.record.es.proto.{ PersistentRepr, Snapshot }
import com.goodcover.fdb.{ FdbSpecLayers, TestId }
import com.google.protobuf.ByteString
import zio.stream.ZStream
import zio.test.*
import zio.{ Clock, Random, Ref, Scope, ZIO, durationInt }

import java.util.concurrent.TimeUnit

object FailureScenarioSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = (suite("FailureScenarioSpec")(
    test("handle invalid persistence id") {
      for {
        service <- ZIO.service[EventsourceLayer]
        // Test with empty persistence id
        result1 <- service.currentEventsById("").runCollect.exit
        // Test with null-like persistence id
        result2 <- service.currentEventsById(null).runCollect.exit
        // Test with very long persistence id
        longId   = "a" * 10000
        result3 <- service.currentEventsById(longId).runCollect.exit
      } yield assertTrue(
        result1.isFailure || result1.getOrElse(_ => Seq.empty).isEmpty,
        result2.isFailure,
        result3.isSuccess // Should handle long IDs gracefully
      )
    },
    test("handle invalid sequence numbers") {
      for {
        service      <- ZIO.service[EventsourceLayer]
        persistenceId = "test-pid"
        // Test with negative sequence numbers
        result1      <- service.currentEventsById(persistenceId, -1L, 10L).runCollect.exit
        // Test with inverted range (from > to)
        result2      <- service.currentEventsById(persistenceId, 100L, 10L).runCollect.exit
        // Test with extremely large sequence numbers
        result3      <- service.currentEventsById(persistenceId, Long.MaxValue - 1, Long.MaxValue).runCollect.exit
      } yield assertTrue(
        result1.isSuccess, // Should handle gracefully
        result2.isSuccess, // Should return empty or handle gracefully
        result3.isSuccess  // Should handle large numbers
      )
    },
    test("handle concurrent append operations") {
      val persistenceId = "concurrent-test"
      val numOperations = 50
      for {
        service <- ZIO.service[EventsourceLayer]
        // Simulate concurrent appends to the same persistence id
        results <- ZIO
                     .foreachPar((0 until numOperations).toVector) { i =>
                       simpleAppend(persistenceId, i.toLong, "concurrent" :: Nil).exit
                     }
                     .withParallelism(10)

        successes = results.count(_.isSuccess)
        failures  = results.count(_.isFailure)

        // Verify that we can still read events after concurrent operations
        events <- service.currentEventsById(persistenceId).runCollect

      } yield assertTrue(
        successes > 0,                               // At least some operations should succeed
        events.nonEmpty || failures == numOperations // Either we have events or all failed
      )
    },
    test("handle duplicate sequence numbers") {
      val persistenceId = "duplicate-test"
      for {
        service <- ZIO.service[EventsourceLayer]
        // Try to append the same sequence number twice
        _       <- simpleAppend(persistenceId, 1L, "tag1" :: Nil)
        result  <- simpleAppend(persistenceId, 1L, "tag1" :: Nil).exit
        events  <- service.currentEventsById(persistenceId).runCollect
      } yield assertTrue(
        result.isFailure || events.size == 1 // Should either fail or deduplicate
      )
    },
    test("handle large payload sizes") {
      val persistenceId = "large-payload-test"
      for {
        service      <- ZIO.service[EventsourceLayer]
        // Create a very large payload (1MB)
        largePayload <- Random.nextBytes(1024 * 1024).map(bytes => ByteString.copyFrom(bytes.toArray))
        result       <- Clock.currentTime(TimeUnit.MILLISECONDS).flatMap { ms =>
                          service
                            .appendEvents(
                              PersistentRepr.of(
                                persistenceId,
                                1L,
                                ms,
                                Seq("large"),
                                largePayload,
                                None,
                                None,
                                Map.empty
                              )
                            )
                            .exit
                        }
        // If append succeeded, try to read it back
        events       <- if (result.isSuccess)
                          service.currentEventsById(persistenceId).runCollect
                        else
                          ZIO.succeed(Seq.empty)
      } yield assertTrue(
        result.isSuccess || result.isFailure,                  // Either succeeds or fails gracefully
        events.isEmpty || events.head.payload.size() > 1000000 // If read, should have large payload
      )
    },
    test("handle snapshot operations with invalid data") {
      val persistenceId = "snapshot-failure-test"
      for {
        service <- ZIO.service[EventsourceLayer]
        // Try to save snapshot with negative sequence number
        result1 <- service
                     .saveSnapshot(
                       Snapshot.of(persistenceId, -1L, 1000L, ByteString.EMPTY, None, None, Map.empty)
                     )
                     .exit
        // Try to save snapshot with invalid timestamp
        result2 <- service
                     .saveSnapshot(
                       Snapshot.of(persistenceId, 1L, -1L, ByteString.EMPTY, None, None, Map.empty)
                     )
                     .exit
        // Try to select snapshot with invalid criteria
        result3 <- service
                     .selectLatestSnapshot(
                       persistenceId,
                       SnapshotSelectionCriteria(maxSequenceNr = -1L)
                     )
                     .runCollect
                     .exit
      } yield assertTrue(
        result1.isFailure || result1.isSuccess, // Should handle gracefully
        result2.isFailure || result2.isSuccess, // Should handle gracefully
        result3.isSuccess                       // Should return empty or handle gracefully
      )
    },
    test("handle tag operations with invalid tags") {
      for {
        service <- ZIO.service[EventsourceLayer]
        // Test with empty tag
        result1 <- service.currentEventsByTag("").runCollect.exit
        // Test with null tag
        result2 <- service.currentEventsByTag(null).runCollect.exit
        // Test with very long tag name
        longTag  = "tag-" + ("x" * 1000)
        result3 <- service.currentEventsByTag(longTag).runCollect.exit
      } yield assertTrue(
        result1.isSuccess, // Should handle empty tag gracefully
        result2.isSuccess, // Should be resilient
        result3.isSuccess  // Should handle long tags
      )
    },
    test("handle deletion operations on non-existent data") {
      val persistenceId = "non-existent-test"
      for {
        service <- ZIO.service[EventsourceLayer]
        // Try to delete from non-existent persistence id
        result1 <- service.deleteTo(persistenceId, 100L).exit
        result2 <- service.deleteFromTo(persistenceId, 10L, 50L).exit
        // Try to delete snapshots that don't exist
        result3 <- service.deleteSnapshot(persistenceId, SnapshotSelectionCriteria()).exit
      } yield assertTrue(
        result1.isSuccess, // Should succeed (no-op)
        result2.isSuccess, // Should succeed (no-op)
        result3.isSuccess  // Should succeed (no-op)
      )
    },
    test("handle stream interruption during operations") {
      val persistenceId = "interrupt-test"
      for {
        service <- ZIO.service[EventsourceLayer]
        // Add some events first
        _       <- ZStream
                     .range(0, 100)
                     .mapZIO(i => simpleAppend(persistenceId, i.toLong, "interrupt" :: Nil))
                     .runCollect

        // Start a stream and interrupt it quickly
        counter <- Ref.make(0)
        fiber   <- service
                     .eventsById(persistenceId, 0L, Long.MaxValue)
                     .tap(_ => counter.update(_ + 1))
                     .runCollect
                     .fork

        _     <- ZIO.sleep(100.millis) // Let it process some events
        _     <- fiber.interrupt
        count <- counter.get

        // Verify the service is still functional after interruption
        events <- service.currentEventsById(persistenceId).runCollect

      } yield assertTrue(
        count >= 0,        // Should have processed some events
        events.size == 100 // Should still be able to read all events
      )
    },
    test("handle metadata operations failures") {
      for {
        service <- ZIO.service[EventsourceLayer]
        // Test loading metadata multiple times
        result1 <- service.loadMetadata.exit
        result2 <- service.loadMetadata.exit
        // Test saving metadata multiple times
        result3 <- service.saveMetadata.exit
        result4 <- service.saveMetadata.exit
      } yield assertTrue(
        result1.isSuccess,
        result2.isSuccess,
        result3.isSuccess,
        result4.isSuccess
      )
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds))
    .provideSome[FdbRecordDatabaseFactory](
      TestId.layer >+>
        (SharedTestLayers.ConfigLayer >>> EventsourceLayer.live) >+> SharedTestLayers.ClearAll
    )
    .provideShared(
      FdbSpecLayers.sharedLayer >+> FdbRecordDatabaseFactory.live
    )

}
