


# fdb-client

fdb-client is an open source project that provides zio-based bindings for FoundationDB, a highly available, fault-tolerant key-value store.

In addition to providing a simple interface for interacting with FoundationDB, fdb-client also includes an implementation of an `EventStoreLayer`. 
The `EventStoreLayer` allows you to store and retrieve a sequence of events that are associated with a specific entity,
making it easy to build event-sourced applications on top of FoundationDB.  It also allows retrieval via tags in a global namespace.
This system makes use of the following awesome properties:

- `fdb-record-layer` - A protobuf system written in java that does consistent indexing and splitting all written by the team
        that wrote fdb.
- `fdb-record-es` - Our system which uses the record layer to implement akka/pekko compatible event sourcing that can be
        used outside the system.
- `fdb-record-es-pekko` - small shim on top of fdb-record-es for pekko
- `fdb-spark` - a record layer <-> spark translation layer that is minimally implemented, but supports reading and writing
        but not creating or deleting tables.  It also supports microbatch reading for structured streaming.

## Installation
To use fdb-client in your project, add the following dependency to your build.sbt file:

```sbt
libraryDependencies += "com.goodcover.fdb" %% "fdb-core" % "0.2.1"
```

## Usage, raw bindings

To use the fdb-client bindings, you will need to have a FoundationDB cluster up and running. 
You can find instructions for setting up a cluster [here](https://apple.github.io/foundationdb/getting-started.html).

We've also included a `docker-compose.yaml` file to spin up a single cluster.

Once you have a cluster running, you can use it with the `FdbDatabase` class, a typical Layer looks like this:

```scala
program.provide(
  ZLayer.succeed(FoundationDbConfig.default),
  FdbPool.layer,
  FdbDatabase.layer
)
```

From there, you can use the client object to perform key-value operations on the database. 
For example, to set a value for a given key, all together looks like this:

```scala

import com.goodcover.fdb.*
import zio.{Cause, ExitCode, LogLevel, Runtime, Scope, URIO, ZIO, ZIOAppArgs, ZIOAppDefault, ZLayer, ZLogger}

object App extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = Runtime.removeDefaultLoggers >>> Runtime.addLogger(
    ZLogger.default.map(println(_)).filterLogLevel(_ >= LogLevel.Debug)
  )

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      ZLayer.succeed(FoundationDbConfig.default),
      FdbPool.layer,
      FdbDatabase.layer
    )

  private val key = "key1".getBytes

  def program =
    for {
      _ <- transact(FdbTxn.set(key, "value1".getBytes))
      result <- transact(FdbTxn.get(key).map(_.map(new String(_))))
    } yield (println(result))
}
```

## Usage, record layer

Arguably, unless you know what you are doing you should be using this, as it does a bunch of really neat things.

Let's look at a complete-ish example

### 1. Define a schema

First step and as part of the base java record layer you're going to need to define a schema and with that schema wire it
up to the Pks and indexes that you want. Look at [event sourcing](fdb-record-es/src/main/scala/com/goodcover/fdb/record/es/EventsourceMeta.scala)

### 2. Maybe create a config that will define various paths?

https://github.com/goodcover/fdb-client/blob/aa870541188e3192304ba168b03ace86553e2951/fdb-record-es/src/main/scala/com/goodcover/fdb/record/es/EventsourceLayer.scala#L702-L706 shows 
the separation of a table vs meta path, which seems like a better way to do it.

### 3. Handle the creation/opening of the metadata

This is where the API isn't great but often there's something like this, where essentially given the path

it will create a new store there, or use the metadata already present in the fdb metastore.  `metaRef` is a Ref to the metadata

```scala
  private def loadMetadata = db.runAsync {
    db.loadMetadata(cfg.metaPath, Some(cfg.localFileDescriptor))
  }

  private def getMetaData(implicit trace: Trace): UIO[FdbMetadata] =
    for {
      result <-
        metaRef.get.some
          .orElse(loadMetadata.some)
          .orElseFail(cfg.localMetadata)
          .merge

      _ <- metaRef.setAsync(Some(result))
    } yield result

  private def mkStore(rc: FdbRecordContext)(implicit trace: Trace): Task[FdbRecordStore] =
    getMetaData.flatMap { meta =>
      rc.createOrOpen(meta, cfg.tablePath)
    }
```

In the spark world this is slightly different since we have to assume everything already exists.

### 4. Insert some elements

This is using the scalapb, so we have to convert to java proto, then insert the element and we're done!

```scala
def setValue(key: TagConsumer, value: ExtendedOffset): Task[Unit] = db.runAsync { (ctx: FdbRecordContext) =>
  for {
    store <- mkStore(ctx)
    _     <- store
               .saveRecord(
                 toJavaProto(
                   ConsumerOffset.of(
                     consumerId = key.consumerId.value,
                     tag = key.tag.value,
                     persistenceId = value.persistenceId,
                     sequenceNr = value.sequenceNr,
                     timestamp = value.timestamp
                   )
                 )
               )
  } yield ()
}
```

## Usage, event store layer

You can also use the `EventStoreLayer` class to store and retrieve events for a given entity. 
For example, to store an event for an entity with ID 1:

```scala
import

for {
  _      <- ZIO.log("hello")
  repr   = PersistentRepr("1", 0L, currentMillis, "tag1" :: Nil, bytesOfData)
  _      <- transact(es.appendLog(repr) *> es.appendLog(repr.copy(sequenceNr = 1L)))
  result <- es.getLog("1", 0L).map(_.get)
} yield (assert(repr == result.repr))
```

### Key Design for the `EventStoreLayer`

This is probably the most interesting section

- `fdb-client` uses the `fdb-record-layer` system, which comes with some directory layer (abstraction on top of raw keys)
    type abstractions, but it also comes with indexing which we use heavily
- We also divide the meta storage of the records and the raw storage of the table.  
- Most tables are prefixed with the record type.
  - Generally everything is expected to be that way 

Tables
  - `PersistentRepr` - this table is the main event storage, it should look similar to akka persistence cassandra or friends
  - `Snapshot` - this is for storing snapshots aka the current state to speed up replays

Layout of Messages/Records and their PK and indexes
  - `PersistentRepr` -  (persistentId, sequenceNr)
    - `eventTagIndex` - (tag, timestamp, pid, sequenceNr)
    - `eventTimestampIndex` - (timestamp, pid, sequenceNr)
    - `maxTag` - (tag) -> version (native fdb construct)
    - `maxSeqNr` - (persistenceId) -> max(seqNr) (native fdb construct max function)
  - `Snapshot` - (persistenceId, sequenceNr) 
    - `snapshotTimestamp` - (persistenceId, timestamp)
  - **Global indexes**
    - `globalCountByRt` - counts by record type (`Snapshot` or `PersistentRepr`) 
    - `globalCount` - global counts (`Snapshot` and `PersistentRepr`)

## Spark usage

The `fdb-spark` job provides a few relational operators on top of the record store.  We have to use the record store
or else we'd essentially have to replicate it with some metadata system since FDB is just a bag of Key value pairs.

**We support** 

- Batched reads
- Batched writes
- Microbatch streaming of indexes (primary keys should be possible as well)

**We don't support**
- Create/alter/deletion of schemas
  - Deletion should be possible, but seems pointless without create/alter
- Continuous streaming (should be possible though)

Look in the tests for examples, like this https://github.com/goodcover/fdb-client/blob/v0.2.11/fdb-spark-tests/src/test/scala/org/apache/spark/sql/fdb/test/SparkCatalogSpec.scala

## [Change Log](./CHANGELOG.md)

## [Contributing Guide](./CONTRIBUTING.md)

## Publishing

Run a little utility to increment and tag versions

```bash
./mill -i --ticker=false "releaseProcess.release"
```

## Resources
- Nix examples, https://github.com/fdb-rs/fdb/blob/main/nix/ci/fdb-7.1/default.nix

## Running FDB Locally

```bash
docker-compose up
# Use fdb client
fdbcli --exec "configure new single memory ; status"
# good to go!
```


## License

fdb-client is licensed under the Apache 2.0 License.
