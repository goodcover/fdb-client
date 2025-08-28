package com.goodcover.fdb

import com.apple.foundationdb.directory.DirectorySubspace
import zio.test.SuiteId

import java.util

private[fdb] case class SuiteSubspace(ds: DirectorySubspace, id: SuiteId) {
  def getPath: util.List[String] = ds.getPath
}
