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

//  /**
//   * Boundary keys can return non-keys. This makes it more complicated than if
//   * it simply returned real keys... We repair in two places, one we shave off
//   * all the framing bits for the chunked encoded.
//   *
//   * Two, something like a Versionstamp or a UUID, we pad the bits.
//   */
//  def dividePartitionsByDesiredScan(
//    config: FdbTable.Config,
//    filters: Array[FilteredKeys],
//    keySize: Int,
//    subspace: DirectorySubspace
//  ): ZIO[FdbTxn with Directories, Throwable, Chunk[FdbInputPartition]] = {
//    val flatKeys          = filters.flatMap(_.keys).zipWithIndex
//    val desiredPartitions = config.desiredPartitions
//
//    val mk = { (b: Array[Byte]) =>
//      TupleHelpersGc
//        .ensureContiguousChunks(
//          TupleHelpersGc.extendKeyToValidTuple(TupleHelpersGc.fromBytes(b)),
//          keySize
//        )
//        .pack()
//    }
//
//    val initialFilter =
//      if (flatKeys.nonEmpty)
//        ZIO
//          .foreach(flatKeys) { case (key, i) =>
//            val range = subspace.range(Tuple.from(key))
//            FdbTxn
//              .boundaryKeys(range.begin, range.end)
//              .map(Boundaries.apply(i, Some(key), LocalRange(range), _))
//          }
//          .map(Chunk.fromArray)
//      else {
//        val range = subspace.range()
//        FdbTxn
//          .boundaryKeys(range.begin, range.end)
//          .map(b => Chunk(Boundaries(0, None, LocalRange(range), b)))
//      }
//
//    for {
//      physicalPartitions   <- initialFilter.map(Topology.apply)
//      _                    <- ZIO.logInfo(s"Total partition size = ${Helpers.humanReadableSize(physicalPartitions.totalEstimate, true)}")
//      _                    <- ZIO.logInfo(s"Physical partition count = ${physicalPartitions.totalPhysicalPartitions}")
//      combineRatio          = Math.max(physicalPartitions.totalPhysicalPartitions / desiredPartitions, 1)
//      newPhysicalPartitions =
//        if (combineRatio == 1) physicalPartitions else physicalPartitions.combinePartitions(combineRatio.toInt)
//      _                    <- ZIO.logInfo(s"Adjusted physical partition count = ${newPhysicalPartitions.totalPhysicalPartitions}")
//
//      partitions = newPhysicalPartitions.boundaries.flatMap { boundaries =>
//                     boundaries.boundariesPerKey.map { boundary =>
//                       FdbInputPartition(
//                         partitionId = s"${boundaries.partitionId}-${boundary.seqNr}",
//                         key = boundaries.key.getOrElse(""),
//                         config = config,
//                         servers = boundary.addresses,
//                         parentRange = boundaries.parentRange,
//                         range = LocalRange(mk(boundary.range.begin), mk(boundary.range.end)),
//                         estimatedSize = boundary.estimatedSize
//                       )
//
//                     }
//                   }
//      _         <- ZIO.logInfo(s"Partitions = $partitions")
//
//    } yield partitions
//
//  }

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
