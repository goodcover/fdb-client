package com.goodcover.fdb.es.pekko

import com.goodcover.fdb.record.es.EventsourceLayer
import com.goodcover.fdb.record.es.proto as ES
import com.typesafe.config.Config
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ExtendedActorSystem
import org.apache.pekko.persistence.query.{ EventEnvelope, NoOffset, Offset, ReadJournalProvider, TimestampOffset }
import org.apache.pekko.persistence.query.javadsl
import org.apache.pekko.persistence.query.scaladsl.{
  CurrentEventsByPersistenceIdQuery,
  CurrentEventsByTagQuery,
  EventsByPersistenceIdQuery,
  EventsByTagQuery,
  ReadJournal
}
import org.apache.pekko.stream.scaladsl.Source
import zio.Task
import zio.interop.reactivestreams.Adapters

import java.time.Instant

class FdbQueryReadProvider(system: ExtendedActorSystem, config: Config, configPath: String) extends ReadJournalProvider {
  override def scaladslReadJournal(): ReadJournal =
    new FdbReadJournal(system, config, configPath)

  override def javadslReadJournal(): javadsl.ReadJournal =
    new com.goodcover.fdb.es.pekko.javadsl.FdbReadJournal(system, config, configPath)
}

class FdbReadJournal(override val system: ExtendedActorSystem, sharedConfig: Config, val cfgPath: String)
    extends ReadJournal
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery
    with FdbEventJournalConnector {

  protected lazy val journalConfig: FdbPekkoJournalConfig =
    FdbPekkoJournalConfig(system, sharedConfig, None)

  private def offsetTranslator(offset: Offset) =
    offset match {
      case NoOffset                            => None
      case TimestampOffset(timestamp, _, seen) =>
        val pair  = seen.toVector.sorted.headOption
        val key   = pair.fold("")(_._1)
        val seqNr = pair.fold(0L)(_._2)
        Some(EventsourceLayer.TagOffset(timestamp.toEpochMilli, key, seqNr))
      case x                                   => throw new Exception(s"Expected NoOffset or TimestampOffset, got $x")
    }

  private def convertInternal(ev: ES.PersistentRepr): Task[EventEnvelope] =
    eventToPekko(ev).map { repr =>
      val ts = Instant.ofEpochMilli(repr.timestamp)
      new EventEnvelope(
        TimestampOffset(ts, ts, Map(repr.persistenceId -> repr.sequenceNr)),
        repr.persistenceId,
        repr.sequenceNr,
        repr.payload,
        repr.timestamp
      )
    }

  override def eventsByPersistenceId(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] =
    Source
      .future(
        unsafeRunFuture(
          Adapters.streamToPublisher(
            es.eventsById(persistenceId, fromSequenceNr, toSequenceNr).mapZIO { ev =>
              convertInternal(ev)
            }
          )
        )
      )
      .flatMapConcat(publisher => Source.fromPublisher(publisher))

  override def currentEventsByPersistenceId(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] = Source
    .future(
      unsafeRunFuture(
        Adapters.streamToPublisher(es.currentEventsById(persistenceId, fromSequenceNr, toSequenceNr).mapZIO { ev =>
          convertInternal(ev)
        })
      )
    )
    .flatMapConcat(publisher => Source.fromPublisher(publisher))

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    val translatedOffset = offsetTranslator(offset)
    Source
      .future(
        unsafeRunFuture(
          Adapters.streamToPublisher(es.eventsByTag(tag, translatedOffset).mapZIO(convertInternal))
        )
      )
      .flatMapConcat(publisher => Source.fromPublisher(publisher))
  }

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    val translatedOffset = offsetTranslator(offset)
    Source
      .future(
        unsafeRunFuture(
          Adapters.streamToPublisher(es.currentEventsByTag(tag, translatedOffset).mapZIO(convertInternal))
        )
      )
      .flatMapConcat(publisher => Source.fromPublisher(publisher))
  }
}

object FdbReadJournal {
  final val Identifier = "fdb.query"
}
