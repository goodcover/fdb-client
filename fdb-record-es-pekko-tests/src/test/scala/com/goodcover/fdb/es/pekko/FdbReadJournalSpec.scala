package com.goodcover.fdb.es.pekko

import com.goodcover.fdb.es.pekko.TestTaggingActor.Ack
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{ ActorLogging, ActorRef, ActorSystem, PoisonPill, Props }
import org.apache.pekko.persistence.journal.{ Tagged, WriteEventAdapter }
import org.apache.pekko.persistence.query.{ NoOffset, PersistenceQuery }
import org.apache.pekko.persistence.{ DeleteMessagesSuccess, PersistentActor, RecoveryCompleted, SaveSnapshotSuccess }
import org.apache.pekko.stream.scaladsl.{ Keep, Sink }
import org.apache.pekko.stream.testkit.TestSubscriber
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.testkit.{ ImplicitSender, TestKitBase }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, Suite }
import zio.Unsafe

import scala.collection.immutable
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class TestTagger extends WriteEventAdapter {
  override def manifest(event: Any): String = ""
  override def toJournal(event: Any): Any   = event match {
    case s: String if s.startsWith("a") => Tagged(event, Set("a"))
    case _                              => event
  }
}

object TestTaggingActor {
  case object Ack
  case object Crash
  case object DoASnapshotPlease
  case object SnapShotAck
  case object Stop

  case class TestEx(msg: String) extends RuntimeException(msg) with NoStackTrace

  def props(pId: String, tags: Set[String] = Set(), probe: Option[ActorRef] = None): Props =
    Props(new TestTaggingActor(pId, tags, probe))
}

class TestTaggingActor(val persistenceId: String, tags: Set[String], probe: Option[ActorRef])
    extends PersistentActor
    with ActorLogging {
  import TestTaggingActor.*

  def receiveRecover: Receive = {
    case RecoveryCompleted =>
      probe.foreach(_ ! RecoveryCompleted)
    case _                 =>
  }

  def receiveCommand: Receive = normal

  def normal: Receive = {
    case event: String     =>
      log.debug("Persisting {}", event)
      persist(Tagged(event, tags)) { e =>
        processEvent(e)
        sender() ! Ack
      }
    case Crash             =>
      throw TestEx("oh dear")
    case DoASnapshotPlease =>
      saveSnapshot("i don't have any state :-/")
      context.become(waitingForSnapshot(sender()))
    case Stop              =>
      context.stop(self)

  }

  def waitingForSnapshot(who: ActorRef): Receive = { case SaveSnapshotSuccess(_) =>
    who ! SnapShotAck
    context.become(normal)
  }

  def processEvent: Receive = { case _ =>
  }
}

object TestActor {
  def props(persistenceId: String, journalId: String = "fdb.journal"): Props =
    Props(new TestActor(persistenceId, journalId))

  final case class PersistAll(events: immutable.Seq[String])
  final case class DeleteTo(seqNr: Long)
}

class TestActor(override val persistenceId: String, override val journalPluginId: String) extends PersistentActor {

  var lastDelete: ActorRef = _

  val receiveRecover: Receive = { case evt: String =>
  }

  val receiveCommand: Receive = {
    case cmd: String =>
      persist(cmd) { evt =>
        sender() ! evt + "-done"
      }
    case cmd: Tagged =>
      persist(cmd) { evt =>
        val msg = s"${evt.payload}-done"
        sender() ! msg
      }

    case TestActor.PersistAll(events) =>
      val size    = events.size
      val handler = {
        var count = 0
        (_: String) => {
          count += 1
          if (count == size)
            sender() ! "PersistAll-done"
        }
      }
      persistAll(events)(handler)

    case TestActor.DeleteTo(seqNr) =>
      lastDelete = sender()
      deleteMessages(seqNr)

    case d: DeleteMessagesSuccess =>
      lastDelete ! d
  }
}

class FdbReadJournalSpec
    extends TestKitBase
    with Suite
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

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
                                      |
                                      |""".stripMargin)
        ) // test's config
        .withFallback(ConfigFactory.load())
        .resolve()

    val as = ActorSystem("FdbReadJournalSpec", finalConfig)
    as.log.info("Using key spaces: {}", finalConfig.getConfig("fdb").getString("testName"))
    as
  }

  lazy val queries: FdbReadJournal =
    PersistenceQuery(system).readJournalFor[FdbReadJournal](FdbReadJournal.Identifier)

  def writeEventsFor(tag: String, persistenceId: String, nrEvents: Int): Unit =
    writeEventsFor(Set(tag), persistenceId, nrEvents)

  def writeEventsFor(tags: Set[String], persistenceId: String, nrEvents: Int): Unit = {
    val ref = system.actorOf(TestTaggingActor.props(persistenceId, tags))
    for (i <- 1 to nrEvents) {
      ref ! s"$persistenceId event-$i"
      expectMsg(Ack)
    }
    watch(ref)
    ref ! PoisonPill
    expectTerminated(ref)
  }

  def eventsPayloads(pid: String): Seq[Any] =
    queries
      .currentEventsByPersistenceId(pid, 0, Long.MaxValue)
      .map(e => e.event)
      .toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue

  def eventsByTag(tag: String): TestSubscriber.Probe[Any] =
    queries.eventsByTag(tag, NoOffset).map(_.event).runWith(TestSink.probe)

  def expectEventsForTag(tag: String, elements: String*): Unit = {
    val probe = queries.eventsByTag(tag, NoOffset).map(_.event).runWith(TestSink.probe)

    probe.request(elements.length + 1)
    elements.foreach(probe.expectNext)
    probe.expectNoMessage(10.millis)
    probe.cancel()
  }

  "Cassandra Read Journal Scala API" must {
    "start eventsByPersistenceId query" in {
      val a = system.actorOf(TestActor.props("a"))
      a ! "a-1"
      expectMsg("a-1-done")

      val src = queries.eventsByPersistenceId("a", 0L, Long.MaxValue)
      src.map(_.persistenceId).runWith(TestSink.probe[Any]).request(10).expectNext("a").cancel()
    }

    "start current eventsByPersistenceId query" in {
      val a = system.actorOf(TestActor.props("b"))
      a ! "b-1"
      expectMsg("b-1-done")

      val src = queries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src.map(_.persistenceId).runWith(TestSink.probe[Any]).request(10).expectNext("b").expectComplete()
    }

    // these tests rely on events written in previous tests
    "start eventsByTag query" in {
      val src = queries.eventsByTag("a", NoOffset)
      src
        .map(_.persistenceId)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .expectNoMessage(100.millis)
        .cancel()
    }

    "start current eventsByTag query" in {
      val src = queries.currentEventsByTag("a", NoOffset)
      src.map(_.persistenceId).runWith(TestSink.probe[Any]).request(10).expectNext("a").expectComplete()
    }

  }

}
