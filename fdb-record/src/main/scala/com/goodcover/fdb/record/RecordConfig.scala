package com.goodcover.fdb.record

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType
import com.apple.foundationdb.record.provider.foundationdb.keyspace.{ KeySpace, KeySpaceDirectory, KeySpacePath }
import com.goodcover.fdb.record.RecordDatabase.FdbMetadata
import com.goodcover.fdb.record.RecordKeySpace.RecordKeySpaceLike
import com.google.protobuf.Descriptors.FileDescriptor

/**
 * This assumes a meta/table namespace, however you could make them the same.
 */
case class RecordConfig(
  recordKeySpace: RecordKeySpaceLike,
  localMetadata: FdbMetadata,
  localFileDescriptor: FileDescriptor,
  persistLocalMetadata: Boolean,
) {
  def this(
    recordKeySpace: RecordKeySpaceLike,
    localMetadata: FdbMetadata,
    localFileDescriptor: FileDescriptor,
  ) = this(recordKeySpace, localMetadata, localFileDescriptor, persistLocalMetadata = true)

  def keyspace: KeySpace      = recordKeySpace.keyspace
  def tablePath: KeySpacePath = recordKeySpace.tablePath
  def metaPath: KeySpacePath  = recordKeySpace.metaPath
}

/**
 * This assumes a meta/table namespace, however you could make them the same.
 */
case class RecordKeySpace(
  keyspace: KeySpace,
  tablePath: KeySpacePath,
  metaPath: KeySpacePath,
) extends RecordKeySpaceLike

object RecordKeySpace {
  trait RecordKeySpaceLike {
    def keyspace: KeySpace
    def tablePath: KeySpacePath
    def metaPath: KeySpacePath
  }

  def makeDefaultConfig(parent: KeySpaceDirectory): RecordKeySpace = {
    val metaPath  = new KeySpaceDirectory("meta", KeyType.STRING, "m")
    val tablePath = new KeySpaceDirectory("table", KeyType.STRING, "t")
    val keyspace  = new KeySpace(parent.addSubdirectory(tablePath).addSubdirectory(metaPath))

    def loop(ks: KeySpaceDirectory, acc: List[String]): List[String] =
      if (ks.getParent == null) ks.getName :: acc
      else loop(ks.getParent, ks.getName :: acc)

    val metaKsPath  = {
      val path = loop(metaPath, Nil).drop(1)
      path.tail.foldLeft(keyspace.path(path.head)) { case (ks, path) => ks.add(path) }
    }
    val tableKsPath = {
      val path = loop(tablePath, Nil).drop(1)
      path.tail.foldLeft(keyspace.path(path.head)) { case (ks, path) => ks.add(path) }
    }

    RecordKeySpace(
      keyspace = keyspace,
      tablePath = tableKsPath,
      metaPath = metaKsPath,
    )
  }
}

object RecordConfig {

  def makeDefaultConfig(
    parent: RecordKeySpace,
    metaData: FdbMetadata,
    localFileDescriptor: FileDescriptor,
    persistLocalMetadata: Boolean = true,
  ): RecordConfig =
    RecordConfig(
      recordKeySpace = parent,
      localMetadata = metaData,
      localFileDescriptor = localFileDescriptor,
      persistLocalMetadata = persistLocalMetadata,
    )

}
