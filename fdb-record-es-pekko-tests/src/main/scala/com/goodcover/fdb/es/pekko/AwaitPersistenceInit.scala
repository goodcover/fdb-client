package com.goodcover.fdb.es.pekko

import org.apache.pekko.actor.{ ActorSystem, PoisonPill, Props }
import org.apache.pekko.persistence.PersistentActor
import org.apache.pekko.testkit.TestProbe

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt

class AwaitPersistenceInit(
  override val persistenceId: String,
  override val journalPluginId: String,
  override val snapshotPluginId: String
) extends PersistentActor {

  def receiveRecover: Receive = { case _ =>
  }

  def receiveCommand: Receive = { case msg =>
    persist(msg) { _ =>
      sender() ! msg
      context.stop(self)
    }
  }

}

object AwaitPersistenceInit {
  def awaitPersistenceInit(system: ActorSystem, journalPluginId: String = "", snapshotPluginId: String = ""): Unit = {
    val probe = TestProbe()(system)
    val t0    = System.nanoTime()
    var n     = 0
    probe.within(45.seconds) {
      probe.awaitAssert(
        {
          n += 1
          val a =
            system.actorOf(
              Props(classOf[AwaitPersistenceInit], "persistenceInit" + n, journalPluginId, snapshotPluginId),
              "persistenceInit" + n
            )
          a.tell("hello", probe.ref)
          try
            probe.expectMsg(5.seconds, "hello")
          catch {
            case t: Throwable =>
              probe.watch(a)
              a ! PoisonPill
              probe.expectTerminated(a, 10.seconds)
              throw t
          }
          system.log
            .debug("awaitPersistenceInit took {} ms {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0), system.name)
        },
        probe.remainingOrDefault,
        2.seconds
      )
    }
  }
}
