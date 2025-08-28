package com.goodcover

import zio.stream.ZStream
import zio.{ Trace, ZIO }

package object fdb {

  def transact[R, A](
    fn: => ZIO[FdbTxn & R, Throwable, A]
  )(implicit trace: Trace): ZIO[FdbDatabase & R, Throwable, A] =
    ZIO.serviceWithZIO[FdbDatabase](_.runAsync[R, A](fn))

  def transact[R, A](
    fn: => ZStream[FdbTxn & R, Throwable, A]
  )(implicit trace: Trace): ZStream[R & FdbStream, Throwable, A] =
    ZStream.serviceWithStream[FdbStream](_.runAsync[R, A](fn))
}
