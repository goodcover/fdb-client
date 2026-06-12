package com.goodcover.fdb.lucene

import com.apple.foundationdb.record.ScanProperties
import com.apple.foundationdb.record.lucene.*
import com.apple.foundationdb.record.lucene.highlight.{ HighlightedTerm, LuceneHighlighting }
import com.apple.foundationdb.record.lucene.search.LuceneQueryParserFactoryProvider
import com.apple.foundationdb.record.provider.foundationdb.{ FDBQueriedRecord, IndexOrphanBehavior }
import com.apple.foundationdb.record.query.expressions.{ Query, QueryComponent }
import com.goodcover.fdb.*
import com.goodcover.fdb.lucene.LuceneSearch.*
import com.goodcover.fdb.record.RecordDatabase.FdbRecordDatabaseFactory
import com.goodcover.fdb.record.lucene.proto.LuceneTest.{ ComplexQuery, Interests }
import com.goodcover.fdb.record.{ BaseLayer, RecordTestLayers }
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.search.{ BooleanClause, BooleanQuery }
import zio.*
import zio.test.*

import java.util.Collections
import scala.jdk.CollectionConverters.*

object LuceneTest extends ZIOSpecDefault {

  val INDEX = "ComplexQuery$text_index"

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

  /** Collect the terms an analyzer produces for a field/text pair. */
  private def analyze(analyzer: Analyzer, field: String, text: String): List[String] = {
    val stream = analyzer.tokenStream(field, text)
    val term   = stream.addAttribute(classOf[CharTermAttribute])
    stream.reset()
    val tokens = scala.collection.mutable.ListBuffer.empty[String]
    while (stream.incrementToken()) tokens += term.toString
    stream.end()
    stream.close()
    tokens.toList
  }

  private def searchQuery(
    text: String,
    additionalFilters: Option[QueryComponent] = None,
    assertSize: Option[Int] = None
  )(implicit trace: Trace): ZIO[BaseLayer, Throwable, Chunk[ComplexQuery]] =
    ZIO.serviceWithZIO[BaseLayer] { layer =>
      layer.withStoreTxn { store =>
        for {
          results <- store.luceneSearch("ComplexQuery", text, additionalFilter = additionalFilters).runCollect
          _       <- assertSize match {
                       case Some(v) => TestResult.liftTestResultToZIO(assertTrue(results.size == v))
                       case None    => ZIO.unit
                     }
        } yield results.map(record => ComplexQuery.newBuilder().mergeFrom(record.record.getRecord).build())
      }
    }

  override def spec: Spec[TestEnvironment with Scope, Any] = (suite("LuceneTest")(
    test("basic query with multiple params") {
      for {
        layer <- ZIO.service[BaseLayer]
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
                     store
                       .luceneSearch("ComplexQuery", "text:(+hell* -dan) AND coverageAmount:[2000 TO 11000]")
                       .runCollect
                       .map(_.map(record => ComplexQuery.newBuilder().mergeFrom(record.record.getRecord).build()))
                   }

      } yield assertTrue( //
        results.size == 1,
        results.head.getId == "3"
      )
    },
    test("highlight query with multiple params") {
      for {
        layer <- ZIO.service[BaseLayer]
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
                             store.luceneHighlightBounds(
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
        layer <- ZIO.service[BaseLayer]
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
                             store.luceneHighlightBounds(
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
        layer <- ZIO.service[BaseLayer]
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
    },
    test("email field uses the email-aware analyzer") {
      for {
        layer <- ZIO.service[BaseLayer]
        _     <- layer.withStoreTxn { store =>
                   for {
                     _ <- store.saveRecord(
                            ComplexQuery
                              .newBuilder()
                              .setId("1")
                              .setText("Hello my name is dan! I really like going over to the well.")
                              .setEmail("dan@goodcover.com")
                              .setCoverageAmount(100)
                              .build()
                          )
                     _ <- store.saveRecord(
                            ComplexQuery
                              .newBuilder()
                              .setId("2")
                              .setText("Hello my name is bill! I really like going over to the well.")
                              .setEmail("bill@example.org")
                              .setCoverageAmount(1_000)
                              .build()
                          )
                   } yield ()
                 }

        // Both analyzers keep the address whole (the record layer's "STANDARD"
        // wrapper is really UAX29URLEmailAnalyzer), so the exact address finds
        // only its record either way...
        exact <- searchQuery(LuceneSearch.LuceneQueryStrings.term("email", "dan@goodcover.com"))
        // ...but only SYNONYM_EMAIL also generates word parts
        // (WordDelimiterFilter with GENERATE_WORD_PARTS | PRESERVE_ORIGINAL),
        // so a fragment like the local part matches through the override.
        part  <- searchQuery("email:bill")

        // Token-level proof that the per-field option routed the email field
        // to the email-aware analyzer: it indexes the parts alongside the
        // whole address, while the default analyzer indexes only the whole.
        indexAnalyzer = registry
                          .getLuceneAnalyzerCombinationProvider(
                            LuceneMetadata.build().getIndex(INDEX),
                            LuceneAnalyzerType.FULL_TEXT,
                            java.util.Collections.emptyMap()
                          )
                          .provideIndexAnalyzer()
                          .getAnalyzer
        emailTokens   = analyze(indexAnalyzer, "email", "dan@goodcover.com")
        textTokens    = analyze(indexAnalyzer, "text", "dan@goodcover.com")
      } yield assertTrue(
        exact.map(_.getId) == Chunk("1"),
        part.map(_.getId) == Chunk("2"),
        emailTokens.toSet == Set("dan@goodcover.com", "dan", "goodcover", "com"),
        textTokens == List("dan@goodcover.com"),
      )
    },
    test("query string helpers") {
      import LuceneSearch.LuceneQueryStrings as Q
      assertTrue(
        Q.term("text", "a+b") == "text:a\\+b",
        Q.terms("key", List("a", "b")) == "key:(a OR b)",
        Q.phrase("text", "say \"hi\"") == "text:\"say \\\"hi\\\"\"",
        Q.prefix("text", "hel") == "text:hel*",
        Q.allOf("a:1", "b:2") == "(a:1 AND b:2)",
        Q.anyOf("a:1", "b:2") == "(a:1 OR b:2)",
      )
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(2.minutes))
    .provideSome[FdbRecordDatabaseFactory](
      TestId.layer >+>
        RecordTestLayers.configLayer(LuceneMetadata.build(), LuceneMetadata.descriptor) >+>
        RecordTestLayers.baseLayer >+>
        RecordTestLayers.clearAll
    )
    .provideShared(
      FdbSpecLayers.sharedLayer ++ FdbLuceneLayers.suiteLayer >+> FdbRecordDatabaseFactory.live
    )

}
