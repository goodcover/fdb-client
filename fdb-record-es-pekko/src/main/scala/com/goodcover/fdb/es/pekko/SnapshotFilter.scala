package com.goodcover.fdb.es.pekko

import org.apache.pekko.persistence.SelectedSnapshot
import zio.RIO

/**
 * Hook into the snapshot store that allows you to filter snapshots to use. This
 * provides a way to intercept and potentially modify or filter snapshots before
 * they are used by the persistence system.
 */
trait SnapshotFilter {

  def apply(maybe: Option[SelectedSnapshot]): RIO[Any, Option[SelectedSnapshot]]

}
