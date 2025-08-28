package com.goodcover.fdb.es.pekko

import org.apache.pekko.persistence.CapabilityFlag
import org.apache.pekko.persistence.journal.JournalSpec
import org.apache.pekko.persistence.query.PersistenceQuery
import zio.Unsafe

class FdbPekkoJournalSpec extends JournalSpec(config = TestConfigProvider.config) {

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

  override def supportsRejectingNonSerializableObjects: CapabilityFlag =
    false // or CapabilityFlag.off

  override def supportsSerialization: CapabilityFlag =
    true // or CapabilityFlag.on
}
