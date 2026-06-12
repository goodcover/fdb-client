package com.goodcover.fdb.lucene

import com.apple.foundationdb.record.lucene.{
  LuceneIndexTypes,
  LucenePlanner,
  LuceneQueryComponent,
  LuceneQueryMultiFieldSearchClause,
  LuceneQueryType,
  LuceneScanBounds,
  LuceneScanQueryParameters
}
import com.apple.foundationdb.record.metadata.{ Index, IndexTypes }
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord
import com.apple.foundationdb.record.query.RecordQuery
import com.apple.foundationdb.record.query.expressions.{ Query, QueryComponent }
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan
import com.apple.foundationdb.record.query.plan.{ PlannableIndexTypes, ScanComparisons }
import com.apple.foundationdb.record.{ EvaluationContext, ExecuteProperties, IsolationLevel }
import com.goodcover.fdb.record.RecordDatabase.FdbRecordStore.Continuable
import com.goodcover.fdb.record.RecordDatabase.{ FdbMetadata, FdbRecordStore }
import com.google.common.collect.Sets
import com.google.protobuf.Message
import org.apache.lucene.queryparser.classic.QueryParserBase
import zio.Trace
import zio.stream.ZStream

import scala.jdk.CollectionConverters.*

/**
 * Lucene query support on top of [[FdbRecordStore]]: the planner/component
 * packaging that every consumer otherwise hand-rolls, plus query-string
 * builders. Import `LuceneSearch.*` for the store syntax.
 *
 * This is also the intended home for structured (non-string) query building.
 */
object LuceneSearch {

  /** The plannable index types most stores want: standard types plus Lucene. */
  val defaultIndexTypes: PlannableIndexTypes = new PlannableIndexTypes(
    Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
    Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
    Sets.newHashSet(IndexTypes.TEXT),
    Sets.newHashSet(LuceneIndexTypes.LUCENE),
  )

  implicit final class LuceneRecordStoreOps(private val store: FdbRecordStore) extends AnyVal {

    /**
     * Plan a Lucene query against `recordType` with the [[LucenePlanner]],
     * without executing it.
     *
     * @param fields
     *   restrict the search to these fields; empty means multi-field search
     *   across the index.
     * @param additionalFilter
     *   a non-Lucene [[QueryComponent]] AND-ed with the Lucene query.
     */
    def lucenePlan(
      recordType: String,
      query: String,
      fields: Seq[String] = Nil,
      additionalFilter: Option[QueryComponent] = None,
      indexTypes: PlannableIndexTypes = defaultIndexTypes,
    ): RecordQueryPlan = {
      val component = new LuceneQueryComponent(
        LuceneQueryType.QUERY,
        query,
        false,
        fields.asJava,
        fields.isEmpty,
      )

      val filter = additionalFilter match {
        case Some(f) => Query.and(component, f)
        case None    => component
      }

      val recordQuery = RecordQuery
        .newBuilder()
        .setRecordType(recordType)
        .setFilter(filter)
        .build()

      val planner = new LucenePlanner(
        store.metadata.metadata,
        store.storeState(),
        indexTypes,
        store.timer,
      )

      planner.plan(recordQuery)
    }

    /**
     * Plan and execute a Lucene query against `recordType` within the current
     * transaction.
     *
     * @param limit
     *   maximum number of rows returned; Int.MaxValue means unlimited.
     * @param continuation
     *   resume from a previous [[Continuable]] continuation.
     */
    def luceneSearch(
      recordType: String,
      query: String,
      limit: Int = Int.MaxValue,
      continuation: Array[Byte] = null,
      fields: Seq[String] = Nil,
      additionalFilter: Option[QueryComponent] = None,
      indexTypes: PlannableIndexTypes = defaultIndexTypes,
    )(implicit trace: Trace): ZStream[Any, Throwable, Continuable[FDBQueriedRecord[Message]]] = {
      val plan = lucenePlan(recordType, query, fields, additionalFilter, indexTypes)

      val executeProperties = ExecuteProperties
        .newBuilder()
        .setIsolationLevel(IsolationLevel.SNAPSHOT)
        .setReturnedRowLimit(limit)
        .build()

      store.executeQuery(plan, continuation, executeProperties, EvaluationContext.EMPTY)
    }

    /**
     * Bind a multi-field full-text scan with highlighting against an index, for
     * use with `scanIndex` + `fetchIndexRecords`.
     */
    def luceneHighlightBounds(
      index: FdbMetadata => Index,
      search: String,
      snippetSize: Int = 10,
    ): LuceneScanBounds = {
      val scan = new LuceneScanQueryParameters(
        ScanComparisons.EMPTY,
        new LuceneQueryMultiFieldSearchClause(LuceneQueryType.QUERY_HIGHLIGHT, search, false),
        null,
        null,
        null,
        new LuceneScanQueryParameters.LuceneQueryHighlightParameters(-1, snippetSize),
      )
      scan.bind(store.underlyingStore, index(store.metadata), EvaluationContext.EMPTY)
    }
  }

  /**
   * Builders for Lucene query strings. These compose textually; combine with
   * `AND`/`OR` or [[allOf]]/[[anyOf]].
   */
  object LuceneQueryStrings {

    /** Escape all Lucene query syntax in `value`. */
    def escape(value: String): String = QueryParserBase.escape(value)

    /** `field:value` with the value escaped. */
    def term(field: String, value: String): String = s"$field:${escape(value)}"

    /** `field:(a OR b OR c)` with each value escaped. */
    def terms(field: String, values: Iterable[String]): String =
      values.map(escape).mkString(s"$field:(", " OR ", ")")

    /** `field:"..."` with quote/backslash escaping suitable inside a phrase. */
    def phrase(field: String, value: String): String = {
      val escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
      s"""$field:"$escaped""""
    }

    /** `field:value*` prefix search with the value escaped. */
    def prefix(field: String, value: String): String = s"$field:${escape(value)}*"

    /** `(a AND b AND c)` */
    def allOf(clauses: String*): String = clauses.mkString("(", " AND ", ")")

    /** `(a OR b OR c)` */
    def anyOf(clauses: String*): String = clauses.mkString("(", " OR ", ")")
  }
}
