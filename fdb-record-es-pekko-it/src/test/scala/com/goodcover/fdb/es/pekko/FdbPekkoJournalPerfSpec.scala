package com.goodcover.fdb.es.pekko

import org.apache.pekko.persistence.CapabilityFlag
import org.apache.pekko.persistence.journal.JournalPerfSpec
import org.apache.pekko.persistence.query.PersistenceQuery
import zio.Unsafe

import scala.concurrent.duration.*

class FdbPekkoJournalPerfSpec extends JournalPerfSpec(config = TestConfigProvider.config) {

  lazy val queries: FdbReadJournal =
    PersistenceQuery(system).readJournalFor[FdbReadJournal](FdbReadJournal.Identifier)

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

  override def awaitDurationMillis: Long = 100.seconds.toMillis

  override def eventsCount: Int = 200

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag =
    false

  override def supportsSerialization: CapabilityFlag =
    true // or CapabilityFlag.on
}
