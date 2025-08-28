package com.goodcover.fdb.es.pekko

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.pekko.actor.{ ActorLogging, ActorRef, ActorSystem, PoisonPill, Props }
import org.apache.pekko.persistence.journal.{ Tagged, WriteEventAdapter }
import org.apache.pekko.persistence.query.{ NoOffset, PersistenceQuery }
import org.apache.pekko.persistence.snapshot.SnapshotStore
import org.apache.pekko.persistence.{
  DeleteMessagesSuccess,
  PersistentActor,
  RecoveryCompleted,
  SaveSnapshotSuccess,
  SelectedSnapshot,
  SnapshotOffer,
  SnapshotSelectionCriteria
}
import org.apache.pekko.stream.scaladsl.{ Keep, Sink }
import org.apache.pekko.stream.testkit.TestSubscriber
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.testkit.{ ImplicitSender, TestKitBase, TestProbe }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, Suite }
import zio.{ RIO, Unsafe, ZIO }

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

object TestSnapshotFailureActor {
  case object Ack
  case object Crash
  case object DoASnapshotPlease
  case object SnapShotAck
  case object Stop
  case object GetState
  case object DoAPoisonedSnapshotPlease

  case class TestEx(msg: String) extends RuntimeException(msg) with NoStackTrace
  case class State(events: List[String] = List.empty, snapshots: Int = 0, poisonRecovery: Boolean = false)

  def props(pId: String, tags: Set[String] = Set(), probe: Option[ActorRef] = None): Props =
    Props(new TestSnapshotFailureActor(pId, tags, probe))
}

class TestSnapshotFailureActor(val persistenceId: String, tags: Set[String], probe: Option[ActorRef])
    extends PersistentActor
    with ActorLogging {
  import TestSnapshotFailureActor.*

  var state = State()

  def receiveRecover: Receive = {
    case SnapshotOffer(metadata, snapshot) =>
      log.info("Recovering from snapshot: {}", snapshot)
      snapshot match {
        case s: State =>
          if (s.poisonRecovery) {
            val ex = TestEx("Poisoned snapshot recovery failed")
            onRecoveryFailure(ex, Some(s))
          } else {
            state = s
          }
          log.info("Successfully recovered state from snapshot: {}", state)
        case other    =>
          log.warning("Unexpected snapshot type: {}", other.getClass)
      }
      probe.foreach(_ ! s"SnapshotRecovered:${metadata.sequenceNr}")

    case event: String =>
      log.debug("Replaying event: {}", event)
      state = state.copy(events = event :: state.events)

    case RecoveryCompleted =>
      log.info("Recovery completed with state: {}", state)
      probe.foreach(_ ! RecoveryCompleted)

    case other =>
      log.debug("Ignoring recovery message: {}", other)
  }

  def receiveCommand: Receive = normal

  def normal: Receive = {
    case event: String =>
      log.debug("Persisting {}", event)
      persist(Tagged(event, tags)) { e =>
        state = state.copy(events = event :: state.events)
        sender() ! Ack
      }

    case Crash =>
      throw TestEx("oh dear")

    case DoASnapshotPlease =>
      val snapshotState = state.copy(snapshots = state.snapshots + 1)
      log.info("Saving snapshot: {}", snapshotState)
      saveSnapshot(snapshotState)
      context.become(waitingForSnapshot(sender()))

    case DoAPoisonedSnapshotPlease =>
      val poisonedState = state.copy(snapshots = state.snapshots + 1, poisonRecovery = true)
      log.info("Saving poisoned snapshot: {}", poisonedState)
      saveSnapshot(poisonedState)
      context.become(waitingForSnapshot(sender()))

    case Stop =>
      context.stop(self)

    case GetState =>
      sender() ! state
  }

  def waitingForSnapshot(who: ActorRef): Receive = {
    case SaveSnapshotSuccess(metadata) =>
      log.info("Snapshot saved successfully: {}", metadata)
      who ! SnapShotAck
      context.become(normal)

    case other =>
      log.warning("Unexpected message while waiting for snapshot: {}", other)
  }
}

class FdbSnapshotReplayFailureSpec
    extends TestKitBase
    with Suite
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  import TestSnapshotFailureActor.*

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    AwaitPersistenceInit.awaitPersistenceInit(system)
  }

  override protected def afterAll(): Unit = {
    Unsafe.unsafe { implicit unsafe =>
      queries.unsafeRunBlocking(queries.es.unsafeDeleteAllRecords)
      queries.shutdown()
    }
    super.afterAll()
  }

  implicit final lazy val system: ActorSystem = {
    // always use this port and keyspace generated here, then test config, then the lifecycle config
    val finalConfig =
      TestConfigProvider.config
        .withFallback(
          ConfigFactory.parseString("""
                                      |
                                      |fdb.journal.event-adapters {
                                      |   test-tagger = com.goodcover.fdb.es.pekko.TestTagger
                                      |}
                                      |fdb.journal.event-adapter-bindings = {
                                      |   "java.lang.String" = test-tagger
                                      |}
                                      |fdb.snapshot-store {
                                      |  snapshotFilter = com.goodcover.fdb.es.pekko.MySnapshotFilter
                                      |}
                                      |pekko.actor.allow-java-serialization=on
                                      |pekko {
                                      |  persistence {
                                      |    journal.plugin = "fdb.journal"
                                      |    snapshot-store.plugin = "testfdb.snapshot-store"
                                      |  }
                                      |}
                                      |""".stripMargin)
        ) // test's config
        .withFallback(ConfigFactory.load())
        .resolve()

    val as = ActorSystem("FdbSnapshotReplayFailureSpec", finalConfig)
    as.log.info("Using key spaces: {}", finalConfig.getConfig("fdb").getString("testName"))
    as
  }

  lazy val queries: FdbReadJournal =
    PersistenceQuery(system).readJournalFor[FdbReadJournal](FdbReadJournal.Identifier)

  "FDB Snapshot Replay Failure" must {
    "handle basic actor lifecycle with snapshots" in {
      val persistenceId = "snapshot-test-1"
      val probe         = TestProbe()

      // Create actor and persist some events
      val ref = system.actorOf(TestSnapshotFailureActor.props(persistenceId, Set("test"), Some(probe.ref)))

      // Send some events
      ref ! "event-1"
      expectMsg(Ack)
      ref ! "event-2"
      expectMsg(Ack)

      // Check state
      ref ! TestSnapshotFailureActor.GetState
      val state1 = expectMsgType[TestSnapshotFailureActor.State]
      state1.events should contain allOf ("event-1", "event-2")

      // Take a snapshot
      ref ! TestSnapshotFailureActor.DoASnapshotPlease
      expectMsg(TestSnapshotFailureActor.SnapShotAck)

      // Add more events after snapshot
      ref ! "event-3"
      expectMsg(Ack)

      // Check state before stopping to ensure event-3 was added
      ref ! TestSnapshotFailureActor.GetState
      val stateBeforeStop = expectMsgType[TestSnapshotFailureActor.State]
      stateBeforeStop.events should contain("event-3")

      // Stop the actor
      watch(ref)
      ref ! PoisonPill
      expectTerminated(ref)

      // Restart the actor - should recover from snapshot + replay events after snapshot
      val ref2 = system.actorOf(TestSnapshotFailureActor.props(persistenceId, Set("test"), Some(probe.ref)))

      // Wait for recovery to complete
      probe.expectMsg(10.seconds, RecoveryCompleted)

      // Check final state
      ref2 ! TestSnapshotFailureActor.GetState
      val finalState = expectMsgType[TestSnapshotFailureActor.State]
      finalState.events should contain allOf ("event-1", "event-2", "event-3")
      finalState.snapshots shouldBe 1

      // Clean up
      watch(ref2)
      ref2 ! PoisonPill
      expectTerminated(ref2)
    }

    "recover using an older snapshot if the latest is filtered out" in {
      val persistenceId = "snapshot-failure-test-1"
      val probe         = TestProbe()

      // Create actor and persist some events
      val ref = system.actorOf(TestSnapshotFailureActor.props(persistenceId, Set("test"), Some(probe.ref)))

      // Send some events
      ref ! "event-1"
      expectMsg(Ack)
      ref ! "event-2"
      expectMsg(Ack)

      // Take a normal snapshot first
      ref ! TestSnapshotFailureActor.DoASnapshotPlease
      expectMsg(TestSnapshotFailureActor.SnapShotAck)

      // Add more events after the normal snapshot
      ref ! "event-3"
      expectMsg(Ack)
      ref ! "event-4"
      expectMsg(Ack)

      // Now take a POISONED snapshot that will fail on recovery
      ref ! TestSnapshotFailureActor.DoAPoisonedSnapshotPlease
      expectMsg(TestSnapshotFailureActor.SnapShotAck)

      // Add one more event after the poisoned snapshot
      ref ! "event-5"
      expectMsg(Ack)

      // Check state before stopping
      ref ! TestSnapshotFailureActor.GetState
      val stateBeforeStop = expectMsgType[TestSnapshotFailureActor.State]
      stateBeforeStop.events should contain allOf ("event-1", "event-2", "event-3", "event-4", "event-5")

      // Stop the actor
      watch(ref)
      ref ! PoisonPill
      expectTerminated(ref)

      // Restart the actor - should fail on poisoned snapshot recovery
      val ref2 = system.actorOf(TestSnapshotFailureActor.props(persistenceId, Set("test"), Some(probe.ref)))

      // Try to send a message to see if the actor is responsive
      ref2 ! TestSnapshotFailureActor.GetState

      // The actor might have crashed, so let's use a shorter timeout
      val finalState = expectMsgType[TestSnapshotFailureActor.State](5.seconds)
      // If we get here, the actor recovered somehow
      finalState.events should contain allOf ("event-1", "event-2", "event-3", "event-4", "event-5")

      // Clean up
      watch(ref2)
      ref2 ! PoisonPill
      expectTerminated(ref2)
    }
  }
}

class MySnapshotFilter extends SnapshotFilter {
  override def apply(maybe: Option[SelectedSnapshot]): RIO[Any, Option[SelectedSnapshot]] =

    maybe match {
      case Some(SelectedSnapshot(_, TestSnapshotFailureActor.State(_, _, true))) => ZIO.none
      case Some(x)                                                               => ZIO.some(x)
      case None                                                                  => ZIO.none
    }
}
