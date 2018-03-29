package raw.runtime.inputformats.bids

/**
  * Human Brain Project - SP8
  * Querying raw BIDS files using RAW
  * Copyright (c) 2017
  * Data Intensive Applications and Systems Labaratory (DIAS)
  *
  * Ecole Polytechnique Federale de Lausanne
  *
  * All Rights Reserved.
  * Permission to use, copy, modify and distribute this software and its
  * documentation is hereby granted, provided that both the copyright
  * notice and this permission notice appear in all copies of the
  * software, derivative works or modified versions, and any portions
  * thereof, and that both notices appear in supporting documentation.
  *
  * This code is distributed in the hope that it will be useful, but
  * WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS AND
  * ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE DISCLAIM ANY LIABILITY OF ANY
  * KIND FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.
  **/

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import raw.PublicException
import com.google.common.io.LittleEndianDataInputStream


import scala.collection.mutable.ArrayBuffer

abstract class BidsParser[+T]() {
  def parse(is: InputStream, tsv_file: String, json_file: String,
            participant: String, nifti_file: String,
            data: Array[Byte]): T

  /* bunch of copy-pasted code from NiftiRecordReader... should refactor */
  def getBoolean(bytes: Array[Byte]): Boolean = {
    if(bytes(0) > 0) true
    else false
  }


  def getByte(bytes: Array[Byte]): Byte = {
    bytes(0)
  }

  def getUByte(bytes: Array[Byte]): Short = {
    val signedVal = getByte(bytes)
    if(signedVal >= 0) signedVal.toShort else (0x100 + signedVal).toShort
  }

  def getShort(bytes: Array[Byte], isLittleEndian: Boolean): Short = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getShort
    else ByteBuffer.wrap(bytes).getShort
  }

  def getUShort(bytes: Array[Byte], isLittleEndian: Boolean): Int = {
    val signedVal = getShort(bytes, isLittleEndian)
    if(signedVal >= 0) signedVal.toInt else (0x10000 + signedVal)
  }

  def getInt(bytes: Array[Byte], isLittleEndian: Boolean): Int = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getInt
    else ByteBuffer.wrap(bytes).getInt
  }

  def getUInt(bytes: Array[Byte], isLittleEndian: Boolean): Long = {
    val signedVal = getInt(bytes, isLittleEndian)
    if(signedVal >= 0) signedVal.toLong else (0x100000000l + signedVal)
  }

  def getLong(bytes: Array[Byte], isLittleEndian: Boolean): Long = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getLong
    else ByteBuffer.wrap(bytes).getLong
  }

  def getULong(bytes: Array[Byte], isLittleEndian: Boolean): BigInt = {
    //Based on: https://stackoverflow.com/questions/21212993/unsigned-variables-in-scala
    if(isLittleEndian) {
      val unsignedLong = ByteBuffer.wrap(bytes.reverse).getLong
      (BigInt(unsignedLong >>> 1) << 1) + (unsignedLong & 1)
    }
    else {
      val unsignedLong = ByteBuffer.wrap(bytes).getLong
      (BigInt(unsignedLong >>> 1) << 1) + (unsignedLong & 1)
    }
  }

  def getFloat(bytes: Array[Byte], isLittleEndian: Boolean): Float = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getFloat
    else ByteBuffer.wrap(bytes).getFloat
  }

  def getDouble(bytes: Array[Byte], isLittleEndian: Boolean): Double = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getDouble
    else ByteBuffer.wrap(bytes).getDouble
  }

  def getBytes(is: InputStream, sampleSize: Int): Array[Byte] = {
    try {
      val buffer = new Array[Byte](sampleSize)
      var totalBytes = 0
      var eof = false
      while (totalBytes < sampleSize && !eof) {
        val bReads = is.read(buffer, totalBytes, sampleSize - totalBytes)
        if (bReads < 0) {
          eof = true
        } else {
          totalBytes += bReads
        }
      }

      if (totalBytes == 0) {
        throw new PublicException("Location appears to be empty")
      }

      if (totalBytes < sampleSize) {
        throw new PublicException("Location has less bytes than expected")
      }

      buffer
    }
  }

  def getInputStream(uriString: String) : InputStream = {

    val file: Path = new Path(uriString)
    val fs: FileSystem = file.getFileSystem(new org.apache.hadoop.conf.Configuration());
    var is: InputStream = fs.open(file)

    if (uriString.endsWith(".gz")) is = new GZIPInputStream(is)

    is
  }


}

final class BidsReader[T](parser: BidsParser[T]) extends RecordReader[NullWritable, T] with StrictLogging {
  private[this] var file: Path = _
  private[this] var parsed: Boolean = false;
  private[this] var currentValue: T = _
  private[this] var fileListSorted: ArrayBuffer[Path] = _
  private[this] var lineNumber: Integer = 0;
  private[this] var ctx: TaskAttemptContext = _

  /*open the file*/
  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val split = inputSplit.asInstanceOf[FileSplit]
    file = split.getPath().getParent()
    ctx = context;

    logger.info("Opening file: " + file)
    val fs: FileSystem = file.getFileSystem(context.getConfiguration)

    var vec = new ArrayBuffer[Path]()
    val iter = fs.listFiles(file, true)
    while (iter.hasNext) {
      vec += iter.next().getPath
    }

    fileListSorted = vec.sortWith((left, right) => left.toUri().compareTo(right.toUri()) > 0)

    lineNumber = 0
  }

  private def hasNext: Boolean = {

    return !parsed
  }

  private[this] def getByteSample(is: InputStream, sampleSize: Int): Array[Byte] = {
    try {
      val buffer = new Array[Byte](sampleSize)
      var totalBytes = 0
      var eof = false
      while (totalBytes < sampleSize && !eof) {
        val bReads = is.read(buffer, totalBytes, sampleSize - totalBytes)
        if (bReads < 0) {
          eof = true
        } else {
          totalBytes += bReads
        }
      }

      if (totalBytes == 0) {
        throw new PublicException("Location appears to be empty")
      }

      if (totalBytes < sampleSize) {
        val trimmedBuffer = new Array[Byte](totalBytes)
        System.arraycopy(buffer, 0, trimmedBuffer, 0, totalBytes)
        trimmedBuffer
      } else {
        buffer
      }

    }
  }

  /*parse the file*/
  override def nextKeyValue(): Boolean = {
    logger.debug("Parsing")
    if (hasNext) {
      var found = false
      while (!found && lineNumber < fileListSorted.size) {
        val nextUri: String = fileListSorted(lineNumber).toString()

        logger.debug(nextUri)

        if (nextUri.endsWith(".nii") || nextUri.endsWith(".nii.gz")) {
          // Temporarily commented to deal with a directory of nifti files not exactly
          // organized as in a BIDS directory

          val relativePath: String = nextUri.substring(file.toUri().toString().length() + 1)
          val firstSubIndex: Integer = relativePath.indexOf("sub")
          val firstSlashIndex: Integer = relativePath.indexOf("/")
          val patientID: String = relativePath.substring(firstSubIndex + "sub".length() + 1,
                        firstSlashIndex - firstSubIndex)

          var is: InputStream = parser.getInputStream(nextUri)
          val data: Array[Byte] = parser.getBytes(is, 348)

          currentValue = parser.parse(is, "", "", "patientID", nextUri.replace("file:/", "file:///"), data)
          found = true
          is.close()
        }
        else if (nextUri.endsWith(".tsv")) {
          currentValue = parser.parse(null, nextUri.replace("file:/", "file:///"), "", "", "",
            new Array[Byte](348));
          found = true;
        }
        else if (nextUri.endsWith(".json")) {
          currentValue = parser.parse(null, "", nextUri.replace("file:/", "file:///"), "", "",
            new Array[Byte](348));
          found = true;
        }

        lineNumber += 1
      }

      if (lineNumber >= fileListSorted.size) {
        parsed = true;
      }

      found
    }
    else false
  }

  /*close the file*/
  override def close(): Unit = {

  }

  override def getCurrentKey: NullWritable = NullWritable.get()

  override def getCurrentValue: T = currentValue

  override def getProgress: Float = 0.5f
}
