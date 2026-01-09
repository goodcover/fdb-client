package org.apache.spark.sql.fdb

import com.apple.foundationdb.record.EndpointType
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.Range as FDBRange
import com.goodcover.fdb.{ FdbTxn, Helpers }
import zio.{ Chunk, NonEmptyChunk, ZIO }

object BaseScanner {

  /** Topology of a set key ranges */
  private case class Topology(boundaries: Chunk[Boundaries]) {
    def totalPhysicalPartitions: Long = boundaries.foldLeft(0L)(_ + _.totalPhysicalPartitions)
    def totalEstimate                 = boundaries.foldLeft(0L)(_ + _.totalEstimate)

    def combinePartitions(by: Int): Topology =
      copy(boundaries.map(_.combinePartitions(by)))

  }

  /** A key's sub key ranges. We only combine within a single key search */
  private case class Boundaries(
    partitionId: Int,
    key: Option[String],
    parentRange: LocalRange,
    boundariesPerKey: NonEmptyChunk[FdbTxn.Boundary]
  ) {
    def totalPhysicalPartitions: Int = boundariesPerKey.size
    def totalEstimate                = boundariesPerKey.foldLeft(0L)(_ + _.estimatedSize)

    def combinePartitions(by: Int): Boundaries = {
      val newBoundariesPerKey = NonEmptyChunk
        .fromIterableOption(
          boundariesPerKey
            .sliding(by, by)
            .map { chunksToCombine =>
              val first = chunksToCombine.head
              val last  = chunksToCombine.last
              val size  = chunksToCombine.foldLeft(0L)(_ + _.estimatedSize)
              first.copy(range = new FDBRange(first.range.begin, last.range.end), estimatedSize = size)
            }
            .toVector
        )
        .getOrElse(boundariesPerKey)

      copy(boundariesPerKey = newBoundariesPerKey)
    }
  }

  def partitionViaPrimaryKey(
    start: Option[Tuple],
    end: Option[Tuple],
    boundaries: Seq[Tuple]
  ): Array[FdbSerializableRange] = {
    val firstBoundary = boundaries.headOption.map(_.pack()).orNull
    val lastBoundary  = boundaries.lastOption.map(_.pack()).orNull
    val startt        = start.map(_.pack()).orNull
    val endt          = end.map(_.pack()).orNull

    val firstEp = if (startt == null) EndpointType.TREE_START else EndpointType.RANGE_INCLUSIVE
    val lastEp  = if (endt == null) EndpointType.TREE_END else EndpointType.RANGE_INCLUSIVE

    val startRange = FdbSerializableRange(startt, firstBoundary, firstEp, EndpointType.RANGE_EXCLUSIVE, null)
    val endRange   = FdbSerializableRange(lastBoundary, endt, EndpointType.RANGE_INCLUSIVE, lastEp, null)

    val middle = if (boundaries.size < 2) {
      Array.empty[FdbSerializableRange]
    } else {
      boundaries
        .sliding(2)
        .map { case Seq(first, second) =>
          val fb = first.pack()
          val sb = second.pack()

          FdbSerializableRange(fb, sb, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE, null)
        }
        .toArray
    }

    startRange +: middle :+ endRange

  }

}
