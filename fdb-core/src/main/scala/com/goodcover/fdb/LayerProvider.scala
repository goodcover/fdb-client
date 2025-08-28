package com.goodcover.fdb

import zio.internal.Platform
import zio.{ Exit, Layer, Runtime, Scope, Trace, Unsafe, ZIO, ZLayer }

/**
 * Used in downstream projects to deal with stringly type configuration systems.
 * Like spark or pekko. This provides a class based way to instantiate the
 * Runtime a certain way while dealing with the systems in their own way.
 *
 * Need to implement a 0-arg constructor for this.
 */
trait LayerProvider {

  def get: ZLayer[Any, Nothing, Any]
}

object LayerProvider {
  class DefaultLayerProvider extends LayerProvider {
    override def get: ZLayer[Any, Nothing, Any] = ZLayer.empty
  }

  /**
   * Stolen from the [[Runtime.unsafe.fromLayer(mkLayers)]] but made blocking
   */
  def mkLayerBlocking[R](layer: Layer[Any, R], existingRuntime: Option[Runtime[Any]] = None)(implicit
    trace: Trace,
    unsafe: Unsafe
  ): Runtime.Scoped[R] = {
    val rt = existingRuntime.getOrElse(Runtime.default)

    val (runtime, shutdown) = rt.unsafe.run {
      ZIO.blocking {
        Scope.make.flatMap { scope =>
          scope.extend[Any](layer.toRuntime).flatMap { acquire =>
            val finalizer = () =>
              rt.unsafe.run {
                ZIO.blocking(scope.close(Exit.unit).uninterruptible.unit)
              }.getOrThrowFiberFailure()

            ZIO.succeed(Platform.addShutdownHook(finalizer)).as((acquire, finalizer))
          }
        }
      }
    }.getOrThrowFiberFailure()

    Runtime.Scoped(runtime.environment, runtime.fiberRefs, runtime.runtimeFlags, () => shutdown())
  }
}
