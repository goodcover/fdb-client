package com.goodcover.fdb.lucene

import com.apple.foundationdb.record.RecordMetaData
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer
import com.apple.foundationdb.record.lucene.{ LuceneFunctionNames, LuceneIndexOptions, LuceneIndexTypes }
import com.apple.foundationdb.record.metadata.Key.Expressions.*
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType
import com.apple.foundationdb.record.metadata.{ Index, Key }
import com.goodcover.fdb.record.lucene.proto.*
import com.google.common.collect.ImmutableMap
import com.google.protobuf.Descriptors

object LuceneMetadata {

  protected val COMBINED_SYNONYM_SETS = "COMBINED_SYNONYM_SETS"

  def build(): RecordMetaData = {
    val metaDataBuilder = RecordMetaData
      .newBuilder()
      .setRecords(LuceneTestProto.javaDescriptor)

    metaDataBuilder
      .getRecordType("ComplexQuery")
      .setPrimaryKey(
        Key.Expressions
          .concat(
            Key.Expressions.recordType(),
            Key.Expressions.field("id"),
          )
      )

    metaDataBuilder.addIndex(
      "ComplexQuery",
      new Index( //
        "ComplexQuery$text_index",
        concat(
          function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
          function(LuceneFunctionNames.LUCENE_SORTED, field("coverageAmount")),
          field("interests", FanType.FanOut).nest( //
            function(LuceneFunctionNames.LUCENE_TEXT, field("firstName")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("lastName")),
          ),
          field("metadata", FanType.FanOut).nest( //
            function(
              LuceneFunctionNames.LUCENE_FIELD_NAME,
              concat(function(LuceneFunctionNames.LUCENE_TEXT, field("value")), field("key"))
            )
          ),
        ),
        LuceneIndexTypes.LUCENE,
        ImmutableMap.of(
          LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION,
          SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
          LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED,
          "true",
        )
      )
    )

    metaDataBuilder.build()
  }

  val descriptor: Descriptors.FileDescriptor = LuceneTestProto.javaDescriptor

}
