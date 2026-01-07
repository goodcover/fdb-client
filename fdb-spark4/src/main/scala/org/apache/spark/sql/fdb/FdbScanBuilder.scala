package org.apache.spark.sql.fdb

import com.apple.foundationdb.record.metadata.expressions.{ GroupingKeyExpression, RecordTypeKeyExpression }
import com.apple.foundationdb.record.query.expressions.{ Query, QueryComponent }
import com.google.protobuf.Descriptors.Descriptor
import org.apache.spark.sql.connector.read.*
import org.apache.spark.sql.fdb.FdbScanBuilder.{ QueryBuilder, compileFilter }
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.*

class FdbScanBuilder(schema: StructType, config: FdbTable.MetadataWith[ReadConf], options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var selectedFields: Array[String] = Array.empty[String]
  private var _pushedFilters                = Array.empty[Filter]

  override def pruneColumns(requiredSchema: StructType): Unit =
    selectedFields = requiredSchema.fieldNames

  private def hasRecordTypeCountIndex = if (pushedFilters.isEmpty && selectedFields.isEmpty) {
    // candidate for count, has empty key (no count aggregation) only on the type
    val indexes = config.recordMetaData.metadata.getRecordMetaData.getAllIndexes.asScala
    indexes.exists { index =>
      if (index.getType == "count") {
        index.getRootExpression match {
          case gke: GroupingKeyExpression if gke.getGroupingSubKey.isInstanceOf[RecordTypeKeyExpression] => true
          case _                                                                                         => false
        }
      } else false
    }
  } else false

  override def build(): Scan = {
    val filters = QueryBuilder(pushedFilters.toSeq, selectedFields.toSeq, hasRecordTypeCountIndex)

    if (config.config.partitionScheme.nonEmpty) {
      new FdbScanViaScheme(schema, filters, config, options)
    } else {
      new FdbScanNaivePlanner(schema, filters, config, options)
    }
  }

  override def pushedFilters: Array[Filter] = this._pushedFilters

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // The partition is either empty, or supports pushDown
    val shouldPushFilters = config.config.partitionScheme.forall { scheme =>
      scheme.scheme.pushDown
    }

    if (shouldPushFilters) {
      val (pushed, unSupported) = filters.partition(f =>
        compileFilter(f, config.recordMetaData.metadata.getRecordType(config.config.recordType).getDescriptor).isDefined
      )
      this._pushedFilters = pushed
      unSupported
    } else {
      filters
    }
  }

}

object FdbScanBuilder {
  case class QueryBuilder(keys: Seq[Filter], selectedFields: Seq[String], hasRecordTypeCountIndex: Boolean)

  object QueryBuilder {
    val empty = QueryBuilder(Seq.empty, Seq.empty, false)
  }

  // This is really dumb for now.
  def compileFilter(f: Filter, meta: Descriptor): Option[QueryComponent] = {
    import org.apache.spark.sql.sources.*

    val specialEqual    = meta.getFields.asScala.filter(_.isRepeated)
    val handleSequences = specialEqual.map(_.getName)

    f match {
      // hasPresence identifies things that can't be empty, like Longs, strings, primitives that are
      // impossible to distinguish between null and 0 for instance.
      case IsNotNull(attribute) if !handleSequences.contains(attribute) =>
        meta.getFields.asScala.find(_.getName == attribute).flatMap { fd =>
          if (!fd.hasPresence)
            None
          else
            Some(Query.field(attribute).notNull())
        }

      case EqualTo(attribute, value) if handleSequences.contains(attribute) =>
        val seq = value.asInstanceOf[Seq[Any]]
        if (seq.length == 1)
          Some(Query.field(attribute).oneOfThem().equalsValue(seq.head))
        else if (seq.isEmpty)
          None
        else
          Some(Query.or(seq.map(f => Query.field(attribute).oneOfThem().equalsValue(f)).asJava))

      case EqualTo(attribute, value) => Some(Query.field(attribute).equalsValue(value))

      case GreaterThan(attribute, value) =>
        Some(Query.field(attribute).greaterThan(value))

      case GreaterThanOrEqual(attribute, value) =>
        Some(Query.field(attribute).greaterThanOrEquals(value))

      case LessThan(attribute, value) =>
        Some(Query.field(attribute).lessThan(value))

      case LessThanOrEqual(attribute, value) =>
        Some(Query.field(attribute).lessThanOrEquals(value))

      case In(attribute, values) =>
        Some(Query.field(attribute).in(values.toList.asJava))

      case Or(left, right) =>
        (compileFilter(left, meta), compileFilter(right, meta)) match {
          case (Some(left), Some(right)) =>
            Some(Query.or(left, right))
          case _                         => None
        }

      case And(left, right) =>
        (compileFilter(left, meta), compileFilter(right, meta)) match {
          case (Some(left), Some(right)) =>
            Some(Query.and(left, right))
          case _                         => None
        }

      case _ => None
    }
  }
}
