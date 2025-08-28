package com.goodcover.fdb.lucene

import com.apple.foundationdb.record.lucene.synonym.{ EnglishSynonymMapConfig, SynonymMapRegistryImpl }
import com.goodcover.fdb.record.RecordDatabase.{ FdbMetadata, FdbRecordDatabase }
import com.goodcover.fdb.record.{ BaseLayer, RecordConfig }
import zio.{ Ref, ZIO, ZLayer }

object FdbLuceneLayers {

  val suiteLayer: ZLayer[Any, Throwable, Unit] = ZLayer.fromZIO {
    for {
      (duration, _) <-
        ZIO
          .attemptBlocking(
            SynonymMapRegistryImpl.instance.getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME)
          )
          .timed
      _             <- ZIO.logWarning(s"FdbLuceneLayer took $duration to setup the English synonym map.")
    } yield ()

  }

  class LuceneLayer(
    override protected val recordDb: FdbRecordDatabase,
    override protected val cfg: RecordConfig,
    override protected val metaRef: Ref[Option[FdbMetadata]]
  ) extends BaseLayer(recordDb, cfg, metaRef)

  object LuceneLayer {
    def apply(
      recordDb: FdbRecordDatabase,
      cfg: RecordConfig,
      metaRef: Ref[Option[FdbMetadata]]
    ): LuceneLayer =
      new LuceneLayer(recordDb, cfg, metaRef)
  }

}
