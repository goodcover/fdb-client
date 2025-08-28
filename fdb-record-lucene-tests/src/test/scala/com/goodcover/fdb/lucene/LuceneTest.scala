package com.goodcover.fdb.lucene

import com.apple.foundationdb.record.lucene.*
import com.apple.foundationdb.record.lucene.highlight.{ HighlightedTerm, LuceneHighlighting }
import com.apple.foundationdb.record.lucene.search.LuceneQueryParserFactoryProvider
import com.apple.foundationdb.record.metadata.{ Index, IndexTypes }
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory
import com.apple.foundationdb.record.provider.foundationdb.{ FDBQueriedRecord, IndexOrphanBehavior }
import com.apple.foundationdb.record.query.RecordQuery
import com.apple.foundationdb.record.query.expressions.{ Query, QueryComponent }
import com.apple.foundationdb.record.query.plan.{ PlannableIndexTypes, ScanComparisons }
import com.apple.foundationdb.record.{ EvaluationContext, ScanProperties }
import com.goodcover.fdb.*
import com.goodcover.fdb.lucene.FdbLuceneLayers.LuceneLayer
import com.goodcover.fdb.record.RecordDatabase.{ FdbMetadata, FdbRecordDatabaseFactory, FdbRecordStore }
import com.goodcover.fdb.record.lucene.proto.LuceneTest.{ ComplexQuery, Interests }
import com.goodcover.fdb.record.{ RecordConfig, RecordKeySpace }
import com.google.common.collect.Sets
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.{ BooleanClause, BooleanQuery }
import zio.*
import zio.test.*

import java.util.Collections
import scala.jdk.CollectionConverters.*

object LuceneTest extends ZIOSpecDefault {

  val INDEX = "ComplexQuery$text_index"

  private val indexTypes = new PlannableIndexTypes(
    Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
    Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
    Sets.newHashSet(IndexTypes.TEXT),
    Sets.newHashSet(LuceneIndexTypes.LUCENE)
  )

  val registry = LuceneAnalyzerRegistryImpl.instance()

  private def doHighlight(queryAnalyzer: Analyzer, indexAnalyzer: Analyzer, text: String, query: String): HighlightedTerm =
    doHighlight(queryAnalyzer, indexAnalyzer, text, query, 3)

  private def doHighlight(
    queryAnalyzer: Analyzer,
    indexAnalyzer: Analyzer,
    text: String,
    queryString: String,
    snippetSize: Int
  ): HighlightedTerm = {
    val highlighter = LuceneHighlighting.makeHighlighter("text", indexAnalyzer, snippetSize)
    val queryParser = LuceneQueryParserFactoryProvider.instance.getParserFactory.createMultiFieldQueryParser(
      Array[String]("text"),
      queryAnalyzer,
      Collections.emptyMap
    )
    queryParser.setAllowLeadingWildcard(true)
    val query       = queryParser.parse(queryString)
    val bq          = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST).build
    val result      = highlighter.highlightWithoutSearcher("text", bq, text, 100)
    result.asInstanceOf[HighlightedTerm]

  }

  private def analyzers = {
    val standardAnalyzerSupplier = () => LuceneAnalyzerWrapper.getStandardAnalyzerWrapper.getAnalyzer
    standardAnalyzerSupplier
  }

  private def searchQuery(
    text: String,
    additionalFilters: Option[QueryComponent] = None,
    assertSize: Option[Int] = None
  )(implicit trace: Trace): ZIO[LuceneLayer, Throwable, Chunk[ComplexQuery]] =
    ZIO.serviceWithZIO[LuceneLayer] { layer =>
      layer.withStoreTxn { store =>
        val qc = new LuceneQueryComponent(
          LuceneQueryType.QUERY,
          text,
          false,
          Seq.empty.asJava,
          true
        )

        val filter = additionalFilters match {
          case Some(f) => Query.and(qc, f)
          case None    => qc
        }

        val query = RecordQuery
          .newBuilder()
          .setRecordType("ComplexQuery")
          .setFilter(filter)
          .build()

        for {
          planner <- ZIO.succeed(
                       new LucenePlanner(
                         store.metadata.metadata,
                         store.storeState(),
                         indexTypes,
                         store.timer
                       )
                     )
          plan    <- ZIO.succeed(planner.plan(query))
          _       <- ZIO.collect(plan.getUsedIndexes.asScala.toList)(i => ZIO.logDebug(s"Used index: $i"))
          results <- store.executeQuery(plan).runCollect
          _       <- assertSize match {
                       case Some(v) => TestResult.liftTestResultToZIO(assertTrue(results.size == v))
                       case None    => ZIO.unit
                     }
        } yield results.map(record => ComplexQuery.newBuilder().mergeFrom(record.record.getRecord).build())
      }
    }

  def fullTextSearch(recordStore: FdbRecordStore, index: FdbMetadata => Index, search: String): LuceneScanBounds = {
    val scan = new LuceneScanQueryParameters(
      ScanComparisons.EMPTY,
      new LuceneQueryMultiFieldSearchClause(LuceneQueryType.QUERY_HIGHLIGHT, search, false),
      null,
      null,
      null,
      new LuceneScanQueryParameters.LuceneQueryHighlightParameters(-1, 10)
    )
    scan.bind(recordStore.underlyingStore, index(recordStore.metadata), EvaluationContext.EMPTY)
  }

  override def spec: Spec[TestEnvironment with Scope, Any] = (suite("LuceneTest")(
    test("basic query with multiple params") {
      for {
        layer <- ZIO.service[LuceneLayer]
        _     <- layer.withStoreTxn { store =>
                   for {
                     _ <-
                       store.saveRecord(
                         ComplexQuery
                           .newBuilder()
                           .setId("1")
                           .setText(
                             "Hello my name is dan! I really like going over to the well."
                           )
                           .setCoverageAmount(100)
                           .build()
                       )
                     _ <-
                       store.saveRecord(
                         ComplexQuery
                           .newBuilder()
                           .setId("2")
                           .setText(
                             "Hello my name is bill! I really like going over to the well."
                           )
                           .setCoverageAmount(1_000)
                           .build()
                       )
                     _ <-
                       store.saveRecord(
                         ComplexQuery
                           .newBuilder()
                           .setId("3")
                           .setText(
                             "Hello my name is bill! I really like going over to the well."
                           )
                           .setCoverageAmount(10_000)
                           .build()
                       )
                   } yield ()
                 }

        results <- layer.withStoreTxn { store =>
                     val qc    =
                       new LuceneQueryComponent(
                         LuceneQueryType.QUERY,
                         "text:(+hell* -dan) AND coverageAmount:[2000 TO 11000]",
                         false,
                         Seq.empty.asJava,
                         true
                       )
                     val query = RecordQuery
                       .newBuilder()
                       .setRecordType("ComplexQuery")
                       .setFilter(qc)
                       .build()

                     for {
                       planner <- ZIO.succeed(
                                    new LucenePlanner(
                                      store.metadata.metadata,
                                      store.storeState(),
                                      indexTypes,
                                      store.timer
                                    )
                                  )
                       plan    <- ZIO.succeed(planner.plan(query))
                       results <- store.executeQuery(plan).runCollect
                     } yield results.map(record => ComplexQuery.newBuilder().mergeFrom(record.record.getRecord).build())
                   }

      } yield assertTrue( //
        results.size == 1,
        results.head.getId == "3"
      )
    },
    test("highlight query with multiple params") {
      for {
        layer <- ZIO.service[LuceneLayer]
        _     <-
          layer.withStoreTxn { store =>
            for {
              _ <- store.saveRecord(
                     ComplexQuery
                       .newBuilder()
                       .setId("1")
                       .setText(
                         "Hello my name is dan! I really like going over to the well."
                       )
                       .setCoverageAmount(100)
                       .build()
                   )
              _ <- store.saveRecord(
                     ComplexQuery
                       .newBuilder()
                       .setId("2")
                       .setText(
                         "Hello my name is bill! I really like going over to the well."
                       )
                       .setCoverageAmount(1_000)
                       .build()
                   )
              _ <- store.saveRecord(
                     ComplexQuery
                       .newBuilder()
                       .setId("3")
                       .setText(
                         "Hello my name is bill! I really like going over to the well."
                       )
                       .addInterests(Interests.newBuilder().setFirstName("Bill").setLastName("Gates").build())
                       .setCoverageAmount(10_000)
                       .build()
                   )
            } yield ()
          }

        results <-
          layer.withStoreTxn { store =>
            ZIO.scoped {

              for {
                sb      <- ZIO.succeed(
                             fullTextSearch(
                               store,
                               _.getIndex(INDEX), // Nesting is with Underscores
                               "text:(+hell* -dan) AND coverageAmount:[2000 TO 11000] AND interests_firstName:Bill"
                             )
                           )
                scan    <- store.scanIndex(_.getIndex(INDEX), sb, null, ScanProperties.FORWARD_SCAN)
                results <-
                  store
                    .fetchIndexRecords(scan, IndexOrphanBehavior.ERROR)
                    .runCollect
              } yield results
            }
          }
        _       <- assertTrue(results.size == 1)
        _       <- ZIO.succeed {
                     results.map { rec =>
                       LuceneHighlighting.highlightedTermsForMessage(FDBQueriedRecord.indexed(rec.record), null).asScala.map {
                         highlights =>
                           highlights
                       }
                     }
                   }

      } yield assertTrue(true)
    },
    test("highlight query different syntax") {
      for {
        layer <- ZIO.service[LuceneLayer]
        _     <-
          layer.withStoreTxn { store =>
            for {
              _ <- store.saveRecord(
                     ComplexQuery
                       .newBuilder()
                       .setId("1")
                       .setText(
                         "Hello my name is BURT! I really like going over to the well."
                       )
                       .setCoverageAmount(100)
                       .build()
                   )
              _ <- store.saveRecord(
                     ComplexQuery
                       .newBuilder()
                       .setId("2")
                       .setText(
                         "Hello my name is bill! I really like going over to the well."
                       )
                       .setCoverageAmount(1_000)
                       .build()
                   )
              _ <- store.saveRecord(
                     ComplexQuery
                       .newBuilder()
                       .setId("3")
                       .setText(
                         "Hello my name is bill! I really like going over to the well."
                       )
                       .addInterests(Interests.newBuilder().setFirstName("Bill").setLastName("Gates").build())
                       .setCoverageAmount(10_000)
                       .build()
                   )
            } yield ()
          }

        results <-
          layer.withStoreTxn { store =>
            ZIO.scoped {

              for {
                sb      <- ZIO.succeed(
                             fullTextSearch(
                               store,
                               _.getIndex(INDEX), // Nesting is with Underscores
                               "text:(+hell* -dan) AND coverageAmount:[* TO 200]"
                             )
                           )
                scan    <- store.scanIndex(_.getIndex(INDEX), sb, null, ScanProperties.FORWARD_SCAN)
                results <-
                  store
                    .fetchIndexRecords(scan, IndexOrphanBehavior.ERROR)
                    .runCollect
              } yield results
            }
          }
        _       <- assertTrue(results.size == 1)
        _       <- ZIO.succeed {
                     results.map { rec =>
                       LuceneHighlighting.highlightedTermsForMessage(FDBQueriedRecord.indexed(rec.record), null).asScala.map {
                         highlights =>
                           highlights
                       }
                     }
                   }

      } yield assertTrue(true)
    },
    test("use metadata queries") {

      for {
        layer <- ZIO.service[LuceneLayer]
        _     <-
          layer.withStoreTxn { store =>
            for {
              _ <- store.saveRecord(
                     ComplexQuery
                       .newBuilder()
                       .setId("1")
                       .setText("Hello my name is steve! I really like going over to the well.")
                       .setCoverageAmount(100)
                       .addMetadata(ComplexQuery.Entry.newBuilder().setKey("key1").setValue("value1").build())
                       .addMetadata(ComplexQuery.Entry.newBuilder().setKey("key2").setValue("value2").build())
                       .addMetadata(ComplexQuery.Entry.newBuilder().setKey("key5").setValue("value5").build())
                       .addInterests(Interests.newBuilder().setFirstName("Bill").setLastName("Gates").build())
                       .build()
                   )
              _ <- store.saveRecord(
                     ComplexQuery
                       .newBuilder()
                       .setId("3")
                       .setText("Hello my name is bill! I really like going over to the well.")
                       .addMetadata(ComplexQuery.Entry.newBuilder().setKey("key3").setValue("value3").build())
                       .addMetadata(ComplexQuery.Entry.newBuilder().setKey("key4").setValue("value4").build())
                       .addMetadata(ComplexQuery.Entry.newBuilder().setKey("key5").setValue("value5").build())
                       .addInterests(Interests.newBuilder().setFirstName("Bill").setLastName("Gates").build())
                       .setCoverageAmount(10_000)
                       .build()
                   )
            } yield ()
          }

        _  <- searchQuery("metadata_key:key1", assertSize = Some(0))
        _  <- searchQuery("metadata_key1:value1", assertSize = Some(1))
        vl <- searchQuery("metadata_key3:value3").map(_.map(_.getId))
        _  <- assertTrue(vl == Chunk("3"))

        _ <- searchQuery("metadata_key5:value5 AND bill", assertSize = Some(2))
        _ <- searchQuery("bill", assertSize = Some(2))

      } yield assertTrue(true)
    }
  ) @@ TestAspect.withLiveClock)
    .provideSome[FdbRecordDatabaseFactory with FdbDatabase](
      TestId.layer >+> ConfigLayer >+> FdbSpecLayers.suiteSubspaceLayer >+> TestKeySpace.live >+> ClearAll
    )
    .provideShared(
      FdbSpecLayers.sharedLayer ++ FdbLuceneLayers.suiteLayer >+> FdbRecordDatabaseFactory.live
    )

  /**
   * Clears all records from the LuceneLayer. This is a scoped layer that will
   * delete all records when the layer is closed.
   */
  def ClearAll: ZLayer[FdbRecordDatabaseFactory with LuceneLayer, Nothing, Unit] =
    ZLayer.scoped {
      for {

        ll <- ZIO.service[LuceneLayer]
        _  <- ZIO.addFinalizer {
                Unsafe.unsafe { implicit unsafe =>
                  ll.unsafeDeleteAllRecords.orDie
                }
              }
      } yield ()

    }

  def ConfigLayer(implicit fn: sourcecode.FullName): ZLayer[TestId with FdbRecordDatabaseFactory, Nothing, LuceneLayer] =
    ZLayer.scoped {
      for {
        testKeySpace <- ZIO.service[TestId]
        id            = testKeySpace.id
        pathPerTest   = s"${fn.value}:$id:${com.goodcover.fdb.BuildInfo.scalaVersion}"
        factory      <- ZIO.service[FdbRecordDatabaseFactory]
        db           <- factory.db.orDie

        directory = new KeySpaceDirectory(s"tests", KeySpaceDirectory.KeyType.STRING, pathPerTest)
        _        <- ZIO.logDebug(s"provisioning the following path '$pathPerTest' in keyspaceDirectory '$directory''")
        ks        = RecordKeySpace.makeDefaultConfig(directory)
        rc        = new RecordConfig(
                      ks,
                      FdbMetadata(LuceneMetadata.build(), ks.tablePath),
                      LuceneMetadata.descriptor,
                      persistLocalMetadata = true
                    )
        ref      <- Ref.make(Option.empty[FdbMetadata])
      } yield LuceneLayer(db, rc, ref)
    }

}
