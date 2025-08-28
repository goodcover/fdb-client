package com.goodcover.fdb.es.pekko.javadsl

import com.goodcover.fdb.es.pekko.FdbReadJournal as SFdbReadJournal
import com.typesafe.config.Config
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ExtendedActorSystem
import org.apache.pekko.persistence.query.{ EventEnvelope, Offset }
import org.apache.pekko.persistence.query.javadsl
import org.apache.pekko.stream.javadsl.Source as JSource

class FdbReadJournal(system: ExtendedActorSystem, config: Config, configPath: String)
    extends javadsl.ReadJournal
    with javadsl.EventsByPersistenceIdQuery
    with javadsl.CurrentEventsByPersistenceIdQuery
    with javadsl.EventsByTagQuery
    with javadsl.CurrentEventsByTagQuery {
  private val sdsl = new SFdbReadJournal(system, config, configPath)

  override def eventsByPersistenceId(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long
  ): JSource[EventEnvelope, NotUsed] = sdsl.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentEventsByPersistenceId(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long
  ): JSource[EventEnvelope, NotUsed] =
    sdsl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def eventsByTag(tag: String, offset: Offset): JSource[EventEnvelope, NotUsed] =
    sdsl.eventsByTag(tag, offset).asJava

  override def currentEventsByTag(tag: String, offset: Offset): JSource[EventEnvelope, NotUsed] =
    sdsl.currentEventsByTag(tag, offset).asJava
}
