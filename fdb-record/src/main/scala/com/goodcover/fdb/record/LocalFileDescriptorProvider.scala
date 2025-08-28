package com.goodcover.fdb.record

import com.google.protobuf.Descriptors.FileDescriptor

trait LocalFileDescriptorProvider {

  def fileDescriptor(): FileDescriptor

}
