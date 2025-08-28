package com.goodcover.fdb

import scala.annotation.tailrec

object Helpers {

  case class NamedUnit(bytes: Long, divisor: Double, unitString: String) {
    def value: Double             = bytes.toDouble / divisor
    override def toString: String = f"${bytes.toDouble / divisor}%.1f $unitString"
  }

  def humanReadableSize(bytes: Long, si: Boolean): NamedUnit = {

    // See https://en.wikipedia.org/wiki/Byte
    val (baseValue, unitStrings) =
      if (si)
        (1000, Vector("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"))
      else
        (1024, Vector("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"))

    @tailrec
    def getExponent(curBytes: Long, baseValue: Int, curExponent: Int = 0): Int =
      if (curBytes < baseValue) curExponent
      else {
        val newExponent = 1 + curExponent
        getExponent(curBytes / (baseValue.toLong * newExponent), baseValue, newExponent)
      }

    val exponent   = getExponent(bytes, baseValue)
    val divisor    = Math.pow(baseValue.toDouble, exponent.toDouble)
    val unitString = unitStrings(exponent)

    // Divide the bytes and show one digit after the decimal point
    NamedUnit(bytes, divisor, unitString)
  }

}
