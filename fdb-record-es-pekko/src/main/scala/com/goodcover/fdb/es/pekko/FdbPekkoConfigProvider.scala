package com.goodcover.fdb.es.pekko

import com.goodcover.fdb.record.es.EventsourceLayer.EventsourceConfig
import zio.ZLayer

/** Can create with a constructor with no args or 1 config arg */
trait FdbPekkoConfigProvider {

  def get(): ZLayer[Any, Throwable, EventsourceConfig]

  def finalizer(): ZLayer[FdbEventJournalConnector.R, Throwable, Unit] = ZLayer.succeed(())

}
