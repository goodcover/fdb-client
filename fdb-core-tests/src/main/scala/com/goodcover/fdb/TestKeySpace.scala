package com.goodcover.fdb

import com.apple.foundationdb.Range as FDBRange
import com.apple.foundationdb.tuple.{ ByteArrayUtil, Tuple }
import zio.{ ZIO, ZLayer }

import scala.annotation.nowarn

private[fdb] case class TestKeySpace(d: SuiteSubspace) {
  val mkIntKey: Int => Array[Byte] = i => d.ds.pack(Tuple.from(i))

  def parentTuple: Array[Byte] =
    d.ds.pack()

  def parentRange: FDBRange =
    d.ds.range()

  def parentTuplePrint: String =
    ByteArrayUtil.printable(parentTuple)

}

object TestKeySpace {
  @nowarn("cat=lint-infer-any")
  val live: ZLayer[SuiteSubspace & FdbDatabase, Nothing, TestKeySpace] = ZLayer.scoped {
    for {
      ss  <- ZIO.service[SuiteSubspace]
      fdb <- ZIO.service[FdbDatabase]
      tks  = new TestKeySpace(ss)
      _   <- ZIO.addFinalizer[Any](fdb.runAsync {
               for {
                 tx <- ZIO.service[FdbTxn]
                 _  <- tx.clear(tks.parentRange)
               } yield ()
             }.orDie)
    } yield tks
  }
}
