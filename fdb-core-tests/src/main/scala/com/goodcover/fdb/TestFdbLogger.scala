package com.goodcover.fdb

import com.goodcover.fdb.FdbDatabase.Transactor
import zio.{ Cause, FiberId, FiberRefs, LogLevel, LogSpan, Trace, ZLogger }

/** Shamelessly pulled from zio flow */
object TestFdbLogger extends ZLogger[String, Unit] {
  private[fdb] val PersistenceId = "persistenceId"
  def apply(
    trace: Trace,
    fiberId: FiberId,
    logLevel: LogLevel,
    message0: () => String,
    cause: Cause[Any],
    context: FiberRefs,
    spans0: List[LogSpan],
    annotations: Map[String, String]
  ): Unit = {
    val sb = new StringBuilder()

    val color = logLevel match {
      case LogLevel.Trace   => Console.BLUE
      case LogLevel.Info    => Console.GREEN
      case LogLevel.Warning => Console.YELLOW
      case LogLevel.Error   => Console.RED
      case _                => Console.CYAN
    }
    sb.append(color)

    sb.append("[" + annotations.getOrElse(Transactor.TxId, annotations.getOrElse("txRId", "")) + "] ")
    sb.append("[" + annotations.getOrElse(PersistenceId, "") + "] ")

    val padding = math.max(0, 30 - sb.size)
    sb.append(" " * padding)
    sb.append(message0())

    val remainingAnnotations = annotations - Transactor.TxId - PersistenceId - "txRId"
    if (remainingAnnotations.nonEmpty) {
      sb.append(" ")

      val it    = remainingAnnotations.iterator
      var first = true

      while (it.hasNext) {
        if (first) {
          first = false
        } else {
          sb.append(" ")
        }

        val (key, value) = it.next()

        sb.append(key)
        sb.append("=[")
        sb.append(value)
        sb.append("]")
      }
    }

    trace match {
      case Trace(location, file, line) =>
        sb.append(" location=")

        appendQuoted(location, sb)

        sb.append(" file=")

        appendQuoted(file, sb)

        sb.append(" line=")
          .append(line): Unit

      case _ => ()
    }

    if (cause != null && cause != Cause.empty) {
      sb.append("\nCause:")
        .append(cause.prettyPrint)
        .append("\n"): Unit
    }

    sb.append(Console.RESET): Unit
    println(sb.toString())
  }

  private def appendQuoted(label: String, sb: StringBuilder): Unit =
    if (label.indexOf(" ") < 0) sb.append(label): Unit
    else sb.append("\"").append(label).append("\""): Unit
}
