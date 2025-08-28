## Changes

### 0.3.1 - NEXT
 - Decent lucene changes to support new features, it actually works pretty great.
 - zio to 2.1.19
 - Auto-saving metadata on every transaction it's not present

### 0.3.0

- Upgrade fdb to `4.2.8.0` this includes some changes to the public api
  - https://github.com/FoundationDB/fdb-record-layer/blob/4.2.8.0/docs/sphinx/source/ReleaseNotes.md#-breaking-changes--4
  - https://github.com/FoundationDB/fdb-record-layer/blob/4.2.8.0/docs/sphinx/source/ReleaseNotes.md#-breaking-changes--3
  - https://github.com/FoundationDB/fdb-record-layer/blob/4.2.8.0/docs/sphinx/source/ReleaseNotes.md#-breaking-changes--2
  - https://github.com/FoundationDB/fdb-record-layer/blob/4.2.8.0/docs/sphinx/source/ReleaseNotes.md#-breaking-changes-
- Upgrade scala to `2.13.16`
- ZIO to `2.1.18`
- Add a lucene module that is pretty bare bones
- Compile with source:3 enabled

### 0.2.19

- Add a function to `deleteFromTo` which is a pretty naive way to delete individual events, but allows you to
  specify both sides of the range.
- Expose `withStoreTransaction` to handle stuff we don't support yet
- Upgrade pekko to 1.0.3
- Update sbt 1.10.5, pgp and scalafmt
- Upgrade fdb to `3.5.556.0` this includes some changes to the public api  

### 0.2.18

- Make the pekko adapter more configurable specifically using a mechanism closer to the akka persistence cassandra one

### 0.2.17

- Expose a native spark count reader that uses the native counting capability

### 0.2.16

- `fdb-record-layer` upgrade from 3.4.520.0 -> 3.4.548.0 [Release Notes](https://github.com/FoundationDB/fdb-record-layer/blob/main/docs/ReleaseNotes.md)

### 0.2.15

- Update zio to latest 2.1.9

### 0.2.14

- Fix up broken partition logic.

### 0.2.13

- Allow overriding `ReadConf` from ScanBuilder, also adding override rules via the `combine` method.

### 0.2.12

- Rejig spark partitioning to support a two custom schemes at the moment, we could also do more
  more complex ones as time goes on.
  - Primary Partition scheme, which forgoes the predicate pushdown and divides based on the boundary keys
  - QueryRestrictionScheme which allows you to divide up partitions by additional predicates
- Add count support if you have the right kind of index
- Add re-usability to the storage pool, create less connection waste
- Renamed `TableConfig` to `ReadConf` 

### 0.2.11

- Add `microbatchSnapshot` for changing isolation levels
- Allow for negative max batch size for saying "I want everything" 

### 0.2.10

- More logging on the streaming side

### 0.2.9

- Add parallel batches `parallelBatches` for inserts that aren't transactional.
- scalafmt everything again
- Some fluidity on microbatch missing a metadata log in spark, it will go back 1 batch then die

### 0.2.8

- Small logging fixes (stream termination was informational and it should be debug)
- Switch to an extensionId type model for FDB Pekko, I think this will simplify the normal case


### 0.2.7

- Add `FDB_CLUSTER_FILE` env to FdbPool to be the highest priority way to configure the location of the cluster file
  - Specifically call out the two env variables in the code and why they exist (to cooperate with the underlying driver) 

### 0.2.6

- Removed a bunch of println

### 0.2.5

- Fix microbatch KeyBytes comparator to use a static shardId instead
- Combine local options with catalog based ones for MicroBatch

### 0.2.4

- Pass more options from the `ScanBuilder` to the `Scan` so we can use it in the MicroBatchStream facility.

### 0.2.3

- Add more pushdown filters for common operations. Also verify things mostly work with comparing fields
  with no presence
- Compile the java artifacts for 11

### 0.2.2

- Fix default values for the spark to `emitDefaultValues` it's 99% of the time what you want to do.

### 0.2.1

- Enable Spark catalogs with namespaces
