package org.apache.spark.sql.fdb

import com.apple.foundationdb.record.provider.foundationdb.keyspace.{ KeySpace, KeySpacePath }
import com.goodcover.fdb.record.LocalFileDescriptorProvider
import org.apache.spark.sql.fdb.ReadConf.KeySpaceProvider
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.*

case class ReadConf(
  recordType: String,
  private val keyspaceProviderClass: Option[String],
  maxRequestsPerSecond: Option[Int],
  batchedTransactionalInserts: Option[Int],
  parallelBatchWrites: Option[Int],
  private val partitionSchemeClass: Option[String],
  options: Map[String, String]
) {

  @transient lazy val keyspaceProvider: KeySpaceProvider =
    keyspaceProviderClass.map { ld =>
      val cls = Class.forName(ld)
      cls.getDeclaredConstructor(classOf[Map[String, String]]).newInstance(options).asInstanceOf[KeySpaceProvider]
    }.getOrElse(
      throw new IllegalStateException(
        """You need to pass in keyspaceProviderClass which implements `KeySpaceProvider`.
The class also passes in single parameter, options of type `Map[String, String]`
          """
      )
    )

  @transient lazy val keySpace: KeySpace              = keyspaceProvider.keySpace
  @transient lazy val tableKeySpacePath: KeySpacePath = keyspaceProvider.tablePath
  @transient lazy val metaKeySpacePath: KeySpacePath  = keyspaceProvider.metaPath

  @transient lazy val partitionScheme: Option[PartitionScheme] =
    partitionSchemeClass map { lpCls =>
      val cls = Class.forName(lpCls)
      cls
        .getDeclaredConstructor(classOf[Map[String, String]])
        .newInstance(this.options)
        .asInstanceOf[PartitionScheme]
    }

  def combine(rc: ReadConf): ReadConf =
    ReadConf(                //
      recordType,            // Don't change
      keyspaceProviderClass, // Don't change
      maxRequestsPerSecond = rc.maxRequestsPerSecond orElse maxRequestsPerSecond,
      batchedTransactionalInserts = rc.batchedTransactionalInserts orElse batchedTransactionalInserts,
      parallelBatchWrites = rc.parallelBatchWrites orElse parallelBatchWrites,
      partitionSchemeClass = rc.partitionSchemeClass orElse partitionSchemeClass,
      options = options ++ rc.options
    )
}

object ReadConf {

  final val KEYSPACE_PROVIDER_CLASS       = "keyspaceProviderClass"
  final val RECORD_TYPE                   = "recordType"
  final val MAX_REQUESTS_PER_SECOND       = "maxRequestsPerSecond"
  final val BATCHED_TRANSACTIONAL_INSERTS = "batchedTransactionalInserts"

  @Deprecated
  final val BATCHED_INSERTS       = "batchedInserts"
  final val PARALLEL_BATCH_WRITES = "parallelBatchWrites"

  @Deprecated
  final val PARALLEL_BATCHES       = "parallelBatches"
  final val PARTITION_SCHEME_CLASS = "partitionSchemeClass"

  def intOpt(options: CaseInsensitiveStringMap)(key: String, keys: String*): Option[Int] = {
    val first = Option(options.get(key)).flatMap(_.toIntOption)

    first.orElse(keys.collectFirst {
      case k if options.containsKey(k) && options.get(k).toIntOption.nonEmpty => options.get(k).toInt
    })
  }

  def strOpt(options: CaseInsensitiveStringMap)(key: String, keys: String*): Option[String] = {
    val first = Option(options.get(key))

    first.orElse(keys.collectFirst {
      case k if options.containsKey(k) => options.get(k)
    })
  }

  def fromOptions(o: CaseInsensitiveStringMap): ReadConf = {
    val keyspaceProviderClass = strOpt(o)(KEYSPACE_PROVIDER_CLASS, "path")
    val recordType            = o.get(RECORD_TYPE)
    val maxRequestsPerSecond  = o.getOrDefault(MAX_REQUESTS_PER_SECOND, "").toIntOption
    val batchedInserts        = intOpt(o)(BATCHED_TRANSACTIONAL_INSERTS, BATCHED_INSERTS)
    val parallelBatches       = intOpt(o)(PARALLEL_BATCH_WRITES, PARALLEL_BATCHES)
    val partitionSchemeClass  = strOpt(o)(PARTITION_SCHEME_CLASS)

    ReadConf(
      recordType = recordType,
      keyspaceProviderClass = keyspaceProviderClass,
      maxRequestsPerSecond = maxRequestsPerSecond,
      batchedTransactionalInserts = batchedInserts,
      parallelBatchWrites = parallelBatches,
      partitionSchemeClass = partitionSchemeClass,
      options = o.asScala.toMap,
    )

  }

  /**
   * Class that you implement to do some configuration takes a single
   * constructor with [[Map[String, String]]]
   *
   * Implement a 1 arg constructor with [[Map[String, String]]]
   */
  trait KeySpaceProvider extends LocalFileDescriptorProvider {
    def keySpace: KeySpace

    def tablePath: KeySpacePath
    def metaPath: KeySpacePath

    override def toString: String =
      s"TABLE - path = $tablePath\nMETA - path = $metaPath\n${"-" * 20}\n$keySpace"
  }

}
