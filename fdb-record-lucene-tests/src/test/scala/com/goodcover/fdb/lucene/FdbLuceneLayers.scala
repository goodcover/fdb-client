package com.goodcover.fdb.lucene

import com.apple.foundationdb.record.lucene.synonym.{ EnglishSynonymMapConfig, SynonymMapRegistryImpl }
import zio.{ ZIO, ZLayer }

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
}
