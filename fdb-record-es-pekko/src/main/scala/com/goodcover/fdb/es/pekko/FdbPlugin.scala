package com.goodcover.fdb.es.pekko

import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabase
import com.goodcover.fdb.record.es.proto as ES
import com.goodcover.fdb.record.es.EventsourceLayer
import com.goodcover.fdb.{ FdbDatabase, FdbPool }
import com.google.protobuf.ByteString
import com.typesafe.config.Config
import org.apache.pekko.actor.{ ActorSystem, ExtendedActorSystem }
import org.apache.pekko.persistence.*
import org.apache.pekko.persistence.journal.*
import org.apache.pekko.persistence.snapshot.*
import org.apache.pekko.serialization.*
import zio.{ RIO, Runtime, Trace, UIO, Unsafe, ZIO }

import scala.concurrent.Future
import scala.util.Try

/**
 * Provides a foundation for interfacing with FoundationDB from a ZIO
 * environment. Handles the setup of necessary layers, database connections, and
 * bridges ZIO tasks with Akka Futures for interacting with the event journal.
 */
trait FdbEventJournalConnector {
  import FdbEventJournalConnector.*

  protected def system: ActorSystem
  def cfgPath: String

  protected val serialization: Serialization       = SerializationExtension(system)
  protected val fdbEsExtension: FdbEsExtensionImpl = FdbEsExtension(system)

  // ZIO Staging
  protected implicit val unsafe: Unsafe = Unsafe.unsafe(a => a)

  protected val runtime: Runtime[FdbEventJournalConnector.R] = fdbEsExtension.runtime(cfgPath)

  // necessary classes
  val db: FdbRecordDatabase = runtime.environment.get[FdbRecordDatabase]
  val es: EventsourceLayer  = runtime.environment.get[EventsourceLayer]

  protected def unsafeRunFuture[R0 <: R, A](zio: => ZIO[R, Throwable, A])(implicit trace: Trace): Future[A] =
    runtime.unsafe.runToFuture(zio)

  def unsafeRunBlocking[R0 <: R, A](zio: => ZIO[R, Throwable, A])(implicit trace: Trace): A =
    runtime.unsafe.run(ZIO.blocking(zio)).getOrThrow()

  protected def eventToPekko(event: ES.PersistentRepr): ZIO[Any, Throwable, PersistentRepr] =
    ZIO
      .fromFuture(_ =>
        serialization.serializerByIdentity.get(event.getSerializationId) match {
          case Some(asyncSerializer: AsyncSerializer) =>
            Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
              asyncSerializer.fromBinaryAsync(event.payload.toByteArray, event.getSerializationManifest)
            }

          case _ =>
            def deserializedEvent: AnyRef =
              // Serialization.deserialize adds transport info
              serialization.deserialize(event.payload.toByteArray, event.getSerializationId, event.getSerializationManifest).get

            Future.successful(deserializedEvent)
        }
      )
      .map { ev =>
        PersistentRepr(
          ev,
          event.sequenceNr,
          event.persistenceId,
          event.getSerializationManifest,
          false,
          writerUuid = event.metadata.getOrElse("writerUuid", "")
        ).withTimestamp(event.timestamp)
          .withMetadata(event.metadata)
      }

  def shutdown(): Unit =
    //    layers.shutdown0()
    ()
}

object FdbEventJournalConnector {
  // Layering from ZIO -> Pekko, aka ZIO[..A] -> Future[A]
  type R = FdbPool & FdbDatabase & FdbRecordDatabase & EventsourceLayer
}

class FdbJournal(val sharedConfig: Config, val cfgPath: String)
    extends AsyncWriteJournal
    with AsyncRecovery
    with FdbEventJournalConnector {
  override protected def system: ActorSystem = context.system

  /** Intentionally overridable */
  protected def timestamp: UIO[Long] = ZIO.clockWith(_.instant.map(_.toEpochMilli))

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] =
    unsafeRunFuture(ZIO.succeed(messages.flatMap(_.payload)).flatMap { aw =>
      val futures = ZIO.foreachPar(aw) { pekkoPr =>
        val (payload, tags) = pekkoPr.payload match {
          case Tagged(payload, tags) => (payload, tags)
          case payload               => (payload, Set.empty[String])
        }
        val event: AnyRef   = payload.asInstanceOf[AnyRef]
        val serializer      = serialization.findSerializerFor(event)
        val serManifest     = Serializers.manifestFor(serializer, event)

        timestamp.flatMap { timestamp =>
          serializer match {
            case asyncSer: AsyncSerializer =>
              Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
                ZIO.fromFuture { _ =>
                  asyncSer.toBinaryAsync(event)
                }.map { bytes =>
                  val serEvent = ByteString.copyFrom(bytes)
                  ES.PersistentRepr.of(
                    pekkoPr.persistenceId,
                    pekkoPr.sequenceNr,
                    timestamp,
                    tags.toSeq,
                    serEvent,
                    Some(serializer.identifier),
                    serializationManifest = Some(serManifest),
                    Map("writerUuid" -> pekkoPr.writerUuid)
                  )
                }
              }

            case _ =>
              // Serialization.serialize adds transport info
              val serEvent = ByteString.copyFrom(serialization.serialize(event).get)

              ZIO.succeed {
                ES.PersistentRepr.of(
                  pekkoPr.persistenceId,
                  pekkoPr.sequenceNr,
                  timestamp,
                  tags.toSeq,
                  serEvent,
                  Some(serializer.identifier),
                  serializationManifest = Some(serManifest),
                  Map("writerUuid" -> pekkoPr.writerUuid)
                )
              }
          }
        }
      }

      futures.flatMap { evs =>
        es.appendEvents(evs)
      }.as(Nil)

    })

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    unsafeRunFuture(es.deleteTo(persistenceId, toSequenceNr))

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
    recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    val maxInt = if (max == Long.MaxValue) Int.MaxValue else max.toInt
    if (max == 0L) Future.successful(())
    else
      unsafeRunFuture(es.currentEventsById(persistenceId, fromSequenceNr, toSequenceNr, maxInt).take(max).runForeach { element =>
        eventToPekko(element).map(recoveryCallback)
      })
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    unsafeRunFuture(
      es.highestSequenceNr(persistenceId, fromSequenceNr)
    )

  override def postStop(): Unit = {
    super.postStop()
    shutdown()
  }
}

class FdbSnapshotStore(val cfg: Config, val cfgPath: String) extends SnapshotStore with FdbEventJournalConnector {
  override protected def system: ActorSystem = context.system

  private val snapshotFilter = fdbEsExtension.snapshotFilter(cfgPath)

  protected def loadAsyncZIO(persistenceId: String, criteria: SnapshotSelectionCriteria): RIO[Any, Option[SelectedSnapshot]] =
    es.selectLatestSnapshot(
      persistenceId,
      EventsourceLayer.SnapshotSelectionCriteria(
        maxSequenceNr = criteria.maxSequenceNr,
        maxTimestamp = criteria.maxTimestamp,
        minSequenceNr = criteria.minSequenceNr,
        minTimestamp = criteria.minTimestamp
      )
    ).runHead
      .flatMap { opt =>
        ZIO
          .fromOption(opt)
          .flatMap { ss =>
            ZIO.fromFuture { _ =>
              serialization.serializerByIdentity.get(ss.getSerializationId) match {
                case Some(asyncSerializer: AsyncSerializer) =>
                  Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
                    asyncSerializer.fromBinaryAsync(ss.payload.toByteArray, ss.getSerializationManifest)
                  }

                case _ =>
                  Future.successful {
                    // Serialization.deserialize adds transport info
                    serialization.deserialize(ss.payload.toByteArray, ss.getSerializationId, ss.getSerializationManifest).get
                  }
              }
            }.mapBoth(
              err => Option(err),
              body => SelectedSnapshot(SnapshotMetadata(ss.persistenceId, ss.sequenceNr, ss.timestamp), body)
            )

          }
          .unsome
          .flatMap { o =>
            snapshotFilter match {
              case Some(value) => value.apply(o)
              case None        => ZIO.succeed(o)
            }

          }
      }

  protected def saveAsyncZIO(metadata: SnapshotMetadata, snapshot: Any): RIO[Any, Unit] = {
    val event: AnyRef = snapshot.asInstanceOf[AnyRef]
    val serializer    = serialization.findSerializerFor(event)
    val serManifest   = Serializers.manifestFor(serializer, event)

    val prg = serializer match {
      case asyncSer: AsyncSerializer =>
        Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
          ZIO.fromFuture { implicit ec =>
            asyncSer.toBinaryAsync(event).map { bytes =>
              val serEvent = ByteString.copyFrom(bytes)
              ES.Snapshot.of(
                metadata.persistenceId,
                metadata.sequenceNr,
                metadata.timestamp,
                serEvent,
                Some(serializer.identifier),
                Option(serManifest),
                Map.empty
              )
            }
          }
        }

      case _ =>
        ZIO.succeed {
          // Serialization.serialize adds transport info
          val serEvent = ByteString.copyFrom(serialization.serialize(event).get)

          ES.Snapshot.of(
            metadata.persistenceId,
            metadata.sequenceNr,
            metadata.timestamp,
            serEvent,
            Some(serializer.identifier),
            Option(serManifest),
            Map.empty
          )
        }
    }

    prg.flatMap(ss => es.saveSnapshot(ss))
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    unsafeRunFuture(saveAsyncZIO(metadata, snapshot))

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    unsafeRunFuture(
      es.deleteSingleSnapshot(
        metadata.persistenceId,
        metadata.sequenceNr
      )
    )

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = unsafeRunFuture(
    es.deleteSnapshot(
      persistenceId,
      EventsourceLayer.SnapshotSelectionCriteria(
        maxSequenceNr = criteria.maxSequenceNr,
        maxTimestamp = criteria.maxTimestamp,
        minSequenceNr = criteria.minSequenceNr,
        minTimestamp = criteria.minTimestamp,
      )
    )
  )

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    unsafeRunFuture(loadAsyncZIO(persistenceId, criteria))

  override def postStop(): Unit = {
    shutdown()
    super.postStop()
  }
}

object FdbSnapshotStore {}
