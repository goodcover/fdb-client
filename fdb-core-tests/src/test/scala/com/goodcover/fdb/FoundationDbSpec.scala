package com.goodcover.fdb

import com.apple.foundationdb.KeySelector
import zio.stream.ZStream
import zio.test.*
import zio.{ Console as _, Scope, * }

object FoundationDbSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = (suite("FoundationDb")(
    test("can open a database, issue a get - layered") {
      for {
        result <- transact(FdbTxn.get("foo".getBytes))
      } yield assertTrue(result.isEmpty)
    },
    test("can open a database, issue a get") {
      for {
        result <- FdbDatabase.runAsync {
                    FdbTxn.get("foo".getBytes)
                  }
      } yield assertTrue(result.isEmpty)
    },
    test("can open a database, perform atomic reads and writes") {
      val instance = java.text.NumberFormat.getInstance
      ZIO.scoped {
        for {
          ks       <- ZIO.service[TestKeySpace]
          mkKey     = ks.mkIntKey
          _        <- ZIO.logInfo(s"Key = ${ks.parentTuplePrint}")
          numInsert = 100_000

          countInsert <-
            ZStream
              .range(0, numInsert, 1024)
              .mapChunksZIO { chunks =>
                FdbDatabase.runAsync {

                  ZIO
                    .foreachPar(chunks) { i =>
                      val value = s"value$i".getBytes
                      ZIO.ifZIO(ZIO.succeed(i % 1000 == 0))(ZIO.logInfo(s"Inserted $i so far"), ZIO.unit) *>
                        FdbTxn.execute(_.set(mkKey(i), value)).as((value.size, 1))
                    }
                }
              }
              .runFold((0, 0)) { case ((sizeAcc, countAcc), (size, count)) =>
                (sizeAcc + size, countAcc + count)
              }
              .timed

          wpsFormatted = instance.format(countInsert._2._2.toDouble / countInsert._1.toMillis * 1000)
          _           <-
            ZIO.logInfo(
              s"Inserted ${countInsert._2._2} elements took ${countInsert._1.toMillis}ms. Writes per second=[$wpsFormatted], bytesTotal=[${countInsert._2._1}b]"
            )
          stream      <- ZIO.service[FdbStream]

          readStream <-
            stream
              .range(
                KeySelector.firstGreaterOrEqual(mkKey(0)),
                KeySelector.firstGreaterOrEqual(mkKey(Int.MaxValue))
              )
              .zipWithIndex
              .runCount
              .timed

          rpsFormatted = instance.format(readStream._2.toDouble / readStream._1.toMillis * 1000)

          locality <- transact(FdbTxn.boundaryKeys(mkKey(0), mkKey(Int.MaxValue)))
          estimate <- transact(FdbTxn.estimatedRangeSizeBytes(mkKey(0), mkKey(Int.MaxValue)))

          _ <- ZIO.logInfo(s"LocalityInfo == $locality")
          _ <- ZIO.logInfo(s"Estimate == $estimate bytes, actual bytes = ${countInsert._2._1} bytes")
          _ <-
            ZIO.logInfo(
              s"Read ${readStream._2} elements took ${readStream._1.toMillis}ms. Reads per second=[$rpsFormatted]"
            )

        } yield assertTrue(countInsert._2._2 == numInsert, readStream._2 == numInsert)
      }
    },
    test("test interrupts") {
      ZIO.scoped {
        for {
          ks         <- ZIO.service[TestKeySpace]
          mkKey       = ks.mkIntKey
          _          <- ZIO.logInfo(s"Key = ${ks.parentTuplePrint}")
          numInsert   = 20
          interruptAt = numInsert - 3

          countInsert <-
            ZStream
              .range(0, numInsert)
              .mapChunksZIO { chunks =>
                transact {

                  ZIO
                    .foreachPar(chunks) { i =>
                      ZIO.ifZIO(ZIO.succeed(i % 10 == 0))(ZIO.logInfo(s"Inserted $i so far"), ZIO.unit) *>
                        FdbTxn.execute(_.set(mkKey(i), s"value$i".getBytes))
                    }
                }
              }
              .runCount
              .timed

          stream <- ZIO.service[FdbStream]

          p <- Promise.make[Throwable, Any]

          readStream <-
            stream
              .range(
                KeySelector.firstGreaterOrEqual(mkKey(0)),
                KeySelector.firstGreaterOrEqual(mkKey(Int.MaxValue))
              )
              .zipWithIndex
              .mapZIO { case (_, i) =>
                p.succeed(()).when(i >= interruptAt)
              }
              .interruptWhen(p)
              .runCount

        } yield assertTrue(readStream >= interruptAt && readStream <= countInsert._2)
      }
    },
    test("test user exceptions cancelling transactions") {
      for {
        ks     <- ZIO.service[TestKeySpace]
        key0    = ks.mkIntKey(0)
        key1    = ks.mkIntKey(1)
        result <- transact(FdbTxn.get(key0))
        _      <- assertTrue(result.isEmpty)
        _      <- transact {
                    for {
                      _ <- FdbTxn.set(key0, "0".getBytes)
                    } yield ()
                  }
        result <- transact(FdbTxn.get(key0))
        _      <- assertTrue(result.nonEmpty)
        result <- transact(FdbTxn.get(key1))
        _      <- assertTrue(result.isEmpty)
        _      <- transact {
                    for {
                      _ <- FdbTxn.set(key1, "1".getBytes)
                      _ <- ZIO.fail(new Exception("key1"))
                    } yield ()
                  }.ignore
        result <- transact(FdbTxn.get(key1))
        _      <- assertTrue(result.isEmpty)
        result <- transact(FdbTxn.get(key0))
        _      <- assertTrue(result.nonEmpty)
      } yield assertTrue(true)
    },
    test("test user exceptions cancelling nested transactions") {
      for {
        ks  <- ZIO.service[TestKeySpace]
        key0 = ks.mkIntKey(0)
        key1 = ks.mkIntKey(1)
        key2 = ks.mkIntKey(2)
        key3 = ks.mkIntKey(3)
        _   <- ZIO.logInfo(ks.parentTuplePrint)
        _   <- transact {
                 for {
                   _ <- FdbTxn.set(key0, "0".getBytes)
                   _ <- transact {
                          for {
                            _ <- FdbTxn.set(key1, "1".getBytes)
                            _ <- ZIO.fail(new Exception("key1"))
                          } yield ()
                        }.ignore
                 } yield ()
               }

        _       <- transact {
                     for {
                       _ <- FdbTxn.set(key2, "2".getBytes)
                       _ <- transact {
                              for {
                                _ <- FdbTxn.set(key3, "3".getBytes)
                              } yield ()
                            }.ignore
                       _ <- ZIO.fail(new Exception("key2"))
                     } yield ()
                   }.ignore
        results <- transact(
                     FdbStream
                       .rangeNoRetry(
                         KeySelector.firstGreaterOrEqual(ks.parentRange.begin),
                         KeySelector.firstGreaterOrEqual(ks.parentRange.end)
                       )
                       .runCollect
                   )

      } yield assertTrue( //
        results.size == 2,
        results.map(_.getValue).map(new String(_)) == Chunk("0", "3")
      )
    },
    test("get system keys") {
      val begin = (Chunk(0xff.toByte, 0xff.toByte) ++ Chunk.fromArray("/connection_string".getBytes)).toArray

      for {
        result <-
          transact {
            FdbTxn.options(_.setReadSystemKeys()) *>
              FdbTxn
                .get(
                  begin
                )
          }
      } yield assertTrue(result.nonEmpty && result.exists(_.nonEmpty))
    }
  ) @@ TestAspect.withLiveClock)
    .provideSome[FdbDatabase & FdbStream](
      FdbSpecLayers.suiteSubspaceLayer >+> TestKeySpace.live
    )
    .provideShared(
      FdbSpecLayers.sharedLayer
    )

}
