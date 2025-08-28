package zio.streams.gc

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import zio.*
import zio.Unsafe.*
import zio.stream.*

import java.util.concurrent.atomic.AtomicReference
import zio.stream.internal.AsyncInputProducer
import zio.UIO
import scala.util.control.NoStackTrace

/**
 * Cribbed from https://github.com/zio/interop-reactive-streams/pull/373
 */
object ConversionAdapters {
  def streamToPublisher[R, E <: Throwable, O](
    stream: => ZStream[R, E, O]
  )(implicit trace: Trace): ZIO[R, Nothing, Publisher[O]] = ZIO.runtime[R].map { runtime => subscriber =>
    if (subscriber == null) {
      throw new NullPointerException("Subscriber must not be null.")
    } else
      unsafe { implicit unsafe =>
        val subscription = new SubscriptionProducer[O](subscriber)
        subscriber.onSubscribe(subscription)
        val _            = runtime.unsafe.fork(
          (stream.toChannel >>> ZChannel.fromZIO(subscription.awaitCompletion).embedInput(subscription)).runDrain
        )
        ()
      }
  }

  private class SubscriptionProducer[A](sub: Subscriber[? >: A])(implicit unsafe: Unsafe)
      extends Subscription
      with AsyncInputProducer[Throwable, Chunk[A], Any] {
    import SubscriptionProducer.State

    private val state: AtomicReference[State[A]]  = new AtomicReference(State.initial[A])
    private val completed: Promise[Nothing, Unit] = Promise.unsafe.make(FiberId.None)

    val awaitCompletion: UIO[Unit] = completed.await

    def request(n: Long): Unit =
      if (n <= 0) sub.onError(new IllegalArgumentException("non-positive subscription request"))
      else {
        state.getAndUpdate {
          case State.Running(demand) => State.Running(demand + n)
          case State.Waiting(_)      => State.Running(n)
          case other                 => other
        } match {
          case State.Waiting(resume) => resume.unsafe.done(ZIO.unit)
          case _                     => ()
        }
      }

    def cancel(): Unit = {
      state.getAndSet(State.Cancelled) match {
        case State.Waiting(resume) => resume.unsafe.done(ZIO.interrupt)
        case _                     => ()
      }
      completed.unsafe.done(ZIO.unit)
    }

    def emit(el: Chunk[A])(implicit trace: zio.Trace): UIO[Any] = ZIO.suspendSucceed {
      ZIO.unless(el.isEmpty) {
        ZIO.suspendSucceed {
          state.getAndUpdate {
            case State.Running(demand) =>
              if (demand > el.size)
                State.Running(demand - el.size)
              else
                State.Waiting(Promise.unsafe.make[Nothing, Unit](FiberId.None))
            case other                 => other
          } match {
            case State.Waiting(resume) =>
              resume.await *> emit(el)
            case State.Running(demand) =>
              if (demand > el.size)
                ZIO.succeed(el.foreach(sub.onNext(_)))
              else
                ZIO.succeed(el.take(demand.toInt).foreach(sub.onNext(_))) *> emit(el.drop(demand.toInt))
            case State.Cancelled       =>
              ZIO.interrupt
          }
        }
      }
    }

    def done(a: Any)(implicit trace: zio.Trace): UIO[Any] = ZIO.suspendSucceed {
      state.getAndSet(State.Cancelled) match {
        case State.Running(_)      => ZIO.succeed(sub.onComplete()) *> completed.succeed(())
        case State.Cancelled       => ZIO.interrupt
        case State.Waiting(resume) => ZIO.succeed(sub.onComplete()) *> resume.interrupt *> completed.succeed(())
      }
    }

    def error(cause: Cause[Throwable])(implicit trace: zio.Trace): UIO[Any] = ZIO.suspendSucceed {
      state.getAndSet(State.Cancelled) match {
        case State.Running(_)      =>
          ZIO.succeed {
            cause.failureOrCause.fold(
              sub.onError,
              c => sub.onError(UpstreamDefect(c))
            )
          } *> completed.succeed(())
        case State.Cancelled       => ZIO.interrupt
        case State.Waiting(resume) =>
          ZIO.succeed {
            cause.failureOrCause.fold(
              sub.onError,
              c => sub.onError(UpstreamDefect(c))
            )
          } *> resume.interrupt *> completed.succeed(())
      }
    }

    def awaitRead(implicit trace: zio.Trace): UIO[Any] = ZIO.unit
  }

  private object SubscriptionProducer {
    sealed trait State[+A]
    object State {
      def initial[A](implicit unsafe: Unsafe): State[A] = Waiting(Promise.unsafe.make[Nothing, Unit](FiberId.None))

      final case class Waiting(resume: Promise[Nothing, Unit]) extends State[Nothing]
      final case class Running(demand: Long)                   extends State[Nothing]
      case object Cancelled                                    extends State[Nothing]
    }
  }

  private final case class UpstreamDefect(cause: Cause[Nothing]) extends NoStackTrace {
    override def getMessage(): String = s"Upsteam defect: ${cause.prettyPrint}"
  }
}
