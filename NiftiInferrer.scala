package raw.inferrer.nifti

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

import com.typesafe.scalalogging.StrictLogging
import raw.PublicException
import raw.compiler.base.source._
import raw.compiler.sql.source._
import raw.inputformats.nifti.NiftiInputFormatDescriptor
import raw.inputformats.InputFormatDescriptor


object NiftiInferrer extends StrictLogging {

  def infer(byteSample: Array[Byte]): (Type, InputFormatDescriptor) = {
    val hdr = new NiftiHeader(byteSample)
    hdr.magic match {
      case "ni1" | "n+1" =>
        def getOutputType = hdr.datatype match {
          case NiftiDataTypes.NIFTI_TYPE_COMPLEX64 => SqlRecordType(Vector(SqlAttrType("val1", SqlFloatType(false)), SqlAttrType("val2", SqlFloatType(false))),false)
          case NiftiDataTypes.NIFTI_TYPE_RGB24 => SqlRecordType(Vector(SqlAttrType("val1", SqlFloatType(false)), SqlAttrType("val2", SqlFloatType(false)), SqlAttrType("val3", SqlFloatType(false))),false)
          case _ => SqlFloatType(false)
        }

        /*we support up to 5 dimensions*/
        val dimsNum: Short = if (hdr.dim(0) < 5) hdr.dim(0) else 4
        val valsNum: Short = if (hdr.dim(0) >= 5) hdr.dim(5) else 1

        System.out.println(valsNum + " " + hdr.dim(0) + " " + hdr.dim(1) + " " +  hdr.dim(2) + " " + hdr.dim(3) + " " + hdr.dim(4) + " "  + hdr.dim(5) + " : hdr dims" )
        System.out.println(hdr.datatype + " " + NiftiDataTypes.NIFTI_TYPE_COMPLEX64 + " " + NiftiDataTypes.NIFTI_TYPE_RGB24 )

        def getRightCollectionDim(curDim: Int): SqlType = {
          if (curDim == dimsNum) valueType
          else SqlRecordType(Vector(SqlAttrType(s"dim${(dimsNum - curDim - 1)}", SqlCollectionType(getRightCollectionDim(curDim + 1), false))),false)
        }

        def valueType: SqlType = {
          var valNum = 0
          var multipleAttrPerVoxelType: Vector[SqlAttrType] = Vector.empty
          for (valNum <- 0 until valsNum) {
            multipleAttrPerVoxelType = multipleAttrPerVoxelType :+ SqlAttrType(s"val${valNum}", getOutputType)
          }

          SqlRecordType(multipleAttrPerVoxelType,false)
        }

        val dataType = SqlCollectionType(getRightCollectionDim(0),false)

        val tipe = SqlCollectionType(SqlRecordType(Vector(SqlAttrType("header", SqlRecordType(hdr.getType,false)), SqlAttrType("data", dataType)), false),false)
        /*we support up to 5 dimensions, the 5th is stored in valsNum*/
        val dimsLength: Array[Short] = Array(hdr.dim(1), hdr.dim(2), hdr.dim(3), hdr.dim(4)) //, hdr.dim(5), hdr.dim(6), hdr.dim(7))
        val scale: Array[Float] = Array(hdr.scl_slope, hdr.scl_inter)
        val format = NiftiInputFormatDescriptor(dimsNum, dimsLength, valsNum, hdr.vox_offset, hdr.littleEndian, hdr.datatype, scale, hdr.bitpix)

        (tipe, format)
      case _ =>
        throw new PublicException("Could not verify nifti1 file")
    }
  }
}
