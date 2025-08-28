package com.goodcover.fdb.record.es

import com.apple.foundationdb.record.metadata.Key.Evaluated.NullStandin
import com.apple.foundationdb.record.metadata.expressions.{ EmptyKeyExpression, GroupingKeyExpression }
import com.apple.foundationdb.record.{ RecordMetaData, RecordMetaDataBuilder }
import com.apple.foundationdb.record.metadata.{ Index, IndexTypes, Key }
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType
import com.goodcover.fdb.record.es.proto.*
import com.google.protobuf.Descriptors

object EventsourceMeta {

  val descriptor: Descriptors.FileDescriptor = FdbrecordProto.javaDescriptor

  def buildMetadata: RecordMetaDataBuilder = {

    val metaDataBuilder = RecordMetaData
      .newBuilder()
      .setRecords(FdbrecordProto.javaDescriptor)

    metaDataBuilder.setSplitLongRecords(true)
    metaDataBuilder.setStoreRecordVersions(true)

    metaDataBuilder
      .getRecordType("PersistentRepr")
      .setPrimaryKey(
        Key.Expressions
          .concat(
            Key.Expressions.recordType(),
            Key.Expressions.field("persistenceId"),
            Key.Expressions.field("sequenceNr", FanType.None, NullStandin.NOT_NULL)
          )
      )

    metaDataBuilder
      .getRecordType("Snapshot")
      .setPrimaryKey(
        Key.Expressions
          .concat(
            Key.Expressions.recordType(),
            Key.Expressions.field("persistenceId"),
            Key.Expressions.field("sequenceNr", FanType.None, NullStandin.NOT_NULL)
          )
      )

    metaDataBuilder.addUniversalIndex(
      new Index("globalCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT)
    )

    metaDataBuilder.addUniversalIndex(
      new Index("globalCountByRt", new GroupingKeyExpression(Key.Expressions.recordType(), 0), IndexTypes.COUNT)
    )

    metaDataBuilder.addIndex(
      "Snapshot",
      new Index(
        "snapshotTimestamp",
        Key.Expressions.concat(
          Key.Expressions.field("persistenceId"),
          Key.Expressions.field("timestamp", FanType.None, NullStandin.NOT_NULL)
        )
      )
    )

    metaDataBuilder.addIndex(
      "PersistentRepr",
      new Index("eventVersionstampIndex", Key.Expressions.version(), IndexTypes.VERSION)
    )

    metaDataBuilder.addIndex(
      "PersistentRepr",
      new Index(
        "maxSeqNr",
        new GroupingKeyExpression(
          Key.Expressions.concat(
            Key.Expressions.field("persistenceId"),
            Key.Expressions.field("sequenceNr", FanType.None, NullStandin.NOT_NULL)
          ),
          1
        ),
        IndexTypes.MAX_EVER_LONG
      )
    )

    metaDataBuilder.addIndex(
      "PersistentRepr",
      new Index(
        "maxTag",
        new GroupingKeyExpression(
          Key.Expressions.concat(
            Key.Expressions.field("tags", FanType.FanOut),
            Key.Expressions.version()
          ),
          1
        ),
        IndexTypes.MAX_EVER_VERSION
      )
    )

    metaDataBuilder.addIndex(
      "PersistentRepr",
      new Index(
        "eventTimestampIndex",
        Key.Expressions.concat( //
          Key.Expressions.field("timestamp", FanType.None, NullStandin.NOT_NULL),
          Key.Expressions.field("persistenceId"),
          Key.Expressions.field("sequenceNr", FanType.None, NullStandin.NOT_NULL)
        )
      )
    )

    metaDataBuilder.addIndex(
      "PersistentRepr",
      new Index(
        "eventTagIndex",
        Key.Expressions.concat(
          Key.Expressions.field("tags", FanType.FanOut),
          Key.Expressions.field("timestamp", FanType.None, NullStandin.NOT_NULL),
          Key.Expressions.field("persistenceId"),
          Key.Expressions.field("sequenceNr", FanType.None, NullStandin.NOT_NULL)
        )
      )
    )

    metaDataBuilder
  }

}
