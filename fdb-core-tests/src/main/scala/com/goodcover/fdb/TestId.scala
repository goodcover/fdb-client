package com.goodcover.fdb

import zio.{ ZIO, ZLayer }

import java.util.concurrent.atomic.AtomicInteger

case class TestId(id: String)

object TestId {

  private val counter = new AtomicInteger(1)

  val layer: ZLayer[Any, Nothing, TestId] = ZLayer.scoped {
    ZIO.succeed(counter.getAndIncrement()).map { id =>
      TestId(id.toString)
    }
  }
}
