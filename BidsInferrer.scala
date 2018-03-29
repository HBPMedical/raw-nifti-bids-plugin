package raw.inferrer.bids

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
  **/

import java.io.InputStream
import java.util.zip.GZIPInputStream

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import raw.PublicException
import raw.compiler.base.source._
import raw.compiler.sql.source._
import raw.inferrer.TextSample
import raw.location._
import raw.inferrer.nifti.NiftiHeader
import raw.inputformats.bids.BidsInputFormatDescriptor
import raw.inputformats.nifti.NiftiInputFormatDescriptor
import raw.inputformats.{Encoding, InputFormatDescriptor, InputFormatHint, UTF_8}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object BidsInferrer extends StrictLogging {

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

    } finally {
      is.close()
    }
  }

  def infer(byteSample: Array[Byte],
            encoding: Encoding,
            location: Location,
            hint: InputFormatHint): (Type, InputFormatDescriptor) = {

    val textSample: TextSample = getTextSample(byteSample, encoding)
    val hdr   = new BidsHeader("", "", "", "", new NiftiHeader(new Array[Byte](384)))

    if (textSample.sample.contains("BIDSVersion")) {
      var file = new org.apache.hadoop.fs.Path(location.rawUri).getParent()

      logger.info("Opening filess: " + file)
      val fs: FileSystem = file.getFileSystem(new org.apache.hadoop.conf.Configuration(true))

      var vec = new ArrayBuffer[Path]()
      val iter = fs.listFiles(file, true)
      while (iter.hasNext) {
        vec += iter.next().getPath
      }

      var maxDimNums : Short = 0
      var maxValNums: Short = 0

      var fileListSorted = vec.sortWith((left, right) => left.toUri().compareTo(right.toUri()) > 0)

      var counter = 0;

      var niftiFileNames = new ArrayBuffer[String]()
      var dimsNum = new ArrayBuffer[Short]()
      var dimsLength = new ArrayBuffer[Array[Short]]()

      var valsNum = new ArrayBuffer[Short]()
      var vox_offset = new ArrayBuffer[Float]()
      var isLittleEndian = new ArrayBuffer[Boolean]()
      var dataType = new ArrayBuffer[Short]()
      var scale = new ArrayBuffer[Array[Float]]()
      var bitpix = new ArrayBuffer[Short]()

      while (counter < fileListSorted.size) {
        val nextUri: String = fileListSorted(counter).toString()

        if (nextUri.endsWith(".nii") || nextUri.endsWith(".nii.gz")) {

          val fs: FileSystem = file.getFileSystem(new  org.apache.hadoop.conf.Configuration());
          var is: InputStream = fs.open(new Path(nextUri))
          if (nextUri.endsWith(".gz")) is = new GZIPInputStream(is)

          val data: Array[Byte] = getByteSample(is, 348);
          System.out.println(nextUri)
          val (tipe, format) = raw.inferrer.nifti.NiftiInferrer.infer(data)

          niftiFileNames += nextUri;
          val NiftiInputFormatDescriptor(d1, d2, d3, d4, d5, d6, d7, d8) = format

          dimsNum += d1
          dimsLength += d2
          valsNum += d3
          vox_offset += d4
          isLittleEndian += d5
          dataType += d6
          scale += d7
          bitpix += d8

          if (d2.length > maxDimNums)
            maxDimNums = d2.length.toShort;

          if (d3 > maxValNums)
            maxValNums = d3
        }

        counter += 1;
      }

      var maxDimLengths: ArrayBuffer[Short] = ArrayBuffer.fill(maxDimNums)(0)

      System.out.println( maxDimLengths )


      for (i <- 0 until dimsLength.size) {
        for (j <-0 until dimsLength(i).length) {
          System.out.println(i + " " + j )
          if (maxDimLengths(j) < dimsLength(i)(j)) {
            maxDimLengths(j) = dimsLength(i)(j)
          }
        }
      }

      val format = BidsInputFormatDescriptor(niftiFileNames.toArray, dimsNum.toArray, dimsLength.toArray,
        valsNum.toArray, vox_offset.toArray, isLittleEndian.toArray, dataType.toArray, scale.toArray,
        bitpix.toArray, maxDimNums, maxValNums, maxDimLengths.toArray)

      def getRightCollectionDim(curDim: Int): SqlType = {
        if (curDim == maxDimNums) valueType
        else SqlRecordType(Vector(SqlAttrType(s"dim${(maxDimNums - curDim - 1)}", SqlCollectionType(getRightCollectionDim(curDim + 1), false))),false)
      }

      def valueType: SqlType = {
        var valNum = 0
        var multipleAttrPerVoxelType: Vector[SqlAttrType] = Vector.empty
        for (valNum <- 0 until maxValNums) {
          multipleAttrPerVoxelType = multipleAttrPerVoxelType :+ SqlAttrType(s"val${valNum}", SqlFloatType(false))
        }

        SqlRecordType(multipleAttrPerVoxelType,false)
      }

      val dataFormat = SqlCollectionType(getRightCollectionDim(0),false)


      //val tipe = SqlCollectionType(SqlRecordType(Vector(SqlAttrType("header", SqlRecordType(hdr.getType,false)), SqlAttrType("data", dataFormat)), false),false)
      val tipeVec = hdr.getType :+ SqlAttrType("data", dataFormat)


      val tipe = SqlCollectionType(SqlRecordType(tipeVec, false), false)
      (tipe, format)
    }
    else throw new PublicException("Could not verify BIDS file")
  }

  def getTextSample(bytes: Array[Byte], enc: Encoding): TextSample = {
    val sample = new String(bytes, Encoding.toName(enc))
    logger.debug("Using sample size: " + bytes.length)
    TextSample(sample, enc, bytes.length)
  }
}
