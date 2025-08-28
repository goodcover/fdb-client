package com.goodcover.fdb.record.es

import com.goodcover.fdb.record.LocalFileDescriptorProvider
import com.goodcover.fdb.record.es.proto.FdbrecordProto
import com.google.protobuf.Descriptors

class EventsourceLocalFileDescriptor extends LocalFileDescriptorProvider {

  override def fileDescriptor(): Descriptors.FileDescriptor = FdbrecordProto.javaDescriptor
}
