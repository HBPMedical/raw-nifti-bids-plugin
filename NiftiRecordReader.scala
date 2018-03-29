package raw.runtime.inputformats.nifti

/**
  * Human Brain Project - SP8
  * Querying raw NIFTI files using RAW
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
  * */

import java.io.{DataInput, DataInputStream, InputStream}
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import com.google.common.io.LittleEndianDataInputStream
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import raw.PublicException


abstract class NiftiParser[+T](val isLittleEndian: Boolean, val hasHeader: Boolean, val vox_offset: Float) {
  def parse(is: InputStream): T

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

  def getShort(bytes: Array[Byte]): Short = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getShort
    else ByteBuffer.wrap(bytes).getShort
  }

  def getUShort(bytes: Array[Byte]): Int = {
    val signedVal = getShort(bytes)
    if(signedVal >= 0) signedVal.toInt else (0x10000 + signedVal)
  }

  def getInt(bytes: Array[Byte]): Int = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getInt
    else ByteBuffer.wrap(bytes).getInt
  }

  def getUInt(bytes: Array[Byte]): Long = {
    val signedVal = getInt(bytes)
    if(signedVal >= 0) signedVal.toLong else (0x100000000l + signedVal)
  }

  def getLong(bytes: Array[Byte]): Long = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getLong
    else ByteBuffer.wrap(bytes).getLong
  }

  def getFloat(bytes: Array[Byte]): Float = {
    if(isLittleEndian) ByteBuffer.wrap(bytes.reverse).getFloat
    else ByteBuffer.wrap(bytes).getFloat
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
}

final class NiftiReader[T](parser: NiftiParser[T]) extends RecordReader[NullWritable, T] with StrictLogging {
  private[this] var file: Path = _
  private[this] var is: InputStream = _
  private[this] var parsed: Boolean = false;
  private[this] var currentValue: T = _

  /*open the file*/
  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val split = inputSplit.asInstanceOf[FileSplit]
    file = split.getPath
    logger.info("Opening file: " + file)
    val fs: FileSystem = file.getFileSystem(context.getConfiguration)
    is = fs.open(file)
    if (file.toUri.getPath.endsWith(".gz")) is = new GZIPInputStream(is)
  }

  private def hasNext: Boolean = {
    logger.info("hasNext")
    return !parsed
  }

  /*parse the file*/
  override def nextKeyValue(): Boolean = {
    logger.info("Parsing")
    if (hasNext) {
//      Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray.slice(0,vox_offset)
      /*I give the header as a byte array in the parse but this is not a problem because the parse will be called only once*/
      //currentValue = parser.parse(Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray);
//      try {

      if(!parser.hasHeader) {
        is.skip(parser.vox_offset.toLong)
      } //skip the header

      val data: DataInput =
      if(parser.isLittleEndian) new LittleEndianDataInputStream(is)
      else new DataInputStream(is)

      currentValue = parser.parse(is)
      parsed = true;
      true
//      }
//      catch {
//        case ex:NiftiExcept => {
//          throw new PublicException ("Error while reading NIFTI file "+file+ ": "+ ex)
//        }
//      }
//      finally is.close()
    } else false
  }

  /*close the file*/
  override def close(): Unit = {
    if (is != null) {
      is.close()
    }
  }

  override def getCurrentKey: NullWritable = NullWritable.get()
  override def getCurrentValue: T = currentValue

  override def getProgress: Float = 0.5f
}
