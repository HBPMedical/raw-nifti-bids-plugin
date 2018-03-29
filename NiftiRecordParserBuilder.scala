package raw.executor.spark.inputformats.nifti

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

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import raw.compiler.L0.source.ListType
import raw.compiler.base.source._
import raw.compiler.core.source.{AttrType, RecordType}
import raw.executor.spark.{CodeGenClassCode, CodeGenContext}
import raw.inferrer.nifti.NiftiDataTypes
import raw.inputformats.InputFormatDescriptor
import raw.inputformats.nifti.NiftiInputFormatDescriptor

import scala.language.implicitConversions

object NiftiRecordParserBuilder extends StrictLogging {

  private[this] val ai = new AtomicInteger()

  def apply(df: InputFormatDescriptor, projectedType: RecordType, hasHeader: Boolean)(implicit codeGenContext: CodeGenContext): CodeGenClassCode = {

    val NiftiInputFormatDescriptor(dimsNum, dimsLength, valsNum, vox_offset, isLittleEndian, dataType, scale, bitpix) = df

    def build(): CodeGenClassCode = {
      val scalaType = codeGenContext.buildType(projectedType)

      val args = new Array[String](projectedType.atts.length)
      var totalFields = 0
      val sourceCode = new StringBuilder()
      for (totalFields <- 0 until projectedType.atts.length) {
        val att = projectedType.atts(totalFields)
        args(totalFields) = "_" + totalFields
        att.idn match {
          case "data" => {
            sourceCode.append(s"val asReadData = \n${addReadVoxelLoop(att.tipe)}\n")
            sourceCode.append(s"val _${totalFields} = Seq({\n${codeGenDataBuilder(att.tipe)}})\n")
          }
          case "header" => sourceCode.append(s"val _${totalFields} = ${codeGenHeaderBuilder(att.tipe)}\n")
          case _ => sourceCode.append(s"val _${totalFields} = hdr.${att.idn}\n")
        }
      }
      sourceCode.append(s"\nnew ${codeGenContext.buildType(projectedType)}(${args.mkString(", ")})")

      val className = "NiftiParser" + ai.getAndIncrement()

      val code = new StringBuilder()
      code.append(
        s"""class $className(isLittleEndian: Boolean, hasHeader: Boolean, vox_offset: Float)
           |extends NiftiParser[$scalaType](isLittleEndian, hasHeader, vox_offset) {\n
           |override def parse(is: InputStream): $scalaType = {\n""")
      if (hasHeader) {
        //        code.append(s"val data: Array[Byte] = new Array[Byte](vox_offset.toInt)\n")
        //        code.append(s"input.readFully(data)\n")
        //        code.append(s"val hdr = new NiftiHeader(data)\n")
        code.append("val data = super.getBytes(is, vox_offset.toInt)\n")
        code.append("val hdr = new NiftiHeader(data)\n")
      }
      code.append(s"${sourceCode.toString()}\n}\n}")

      codeGenContext.addClass(className, code.toString())
      CodeGenClassCode(className, code.toString())
    }

    /*the compiler will use this to convert short+1 to short instead of int*/
    implicit def toShort(x: Int): Short = x.toShort

    /*Read the header*/
    def codeGenHeaderBuilder(record: Type): String = {

      val hdrClassName = codeGenContext.buildType(record)

      val RecordType(atts: Vector[AttrType]) = record
      val fieldDeclarations: Seq[String] = atts.map(x => s"hdr.${x.idn}")
      s"new $hdrClassName(${fieldDeclarations.mkString(",")})"
    }

    /* Generate code to read the voxel data in the right order and return the expected type */
    def getTypes(record: Type) : String = {
      record match {
        case ListType(RecordType(vt)) => {
          codeGenContext.buildType(RecordType(vt))+","+getTypes(vt(0).tipe)
        }
        case _ => ""
      }
    }

    def codeGenDataBuilder(record: Type): String = {
      val code = new StringBuilder()
      var itt = 'i'

      val recordTypes: Array[String] = getTypes(record).split(",")

      for (dimNum <- 1 to dimsNum) {
        code.append(s"val d${dimNum} = for(${(itt + dimNum).toChar} <- 0 until ${dimsLength(dimNum - 1)}) yield {\n")
      }
      code.append(s"new ${recordTypes(dimsNum)}(asReadData")
      code.append("(0)")
      for (dimNum <- dimsNum to 1 by -1) {
        code.append(s"(${(itt + dimNum).toChar})")
      }

      var i =0
      for(i <- 1 until valsNum) {
        code.append(", asReadData")
        code.append(s"(${i})")
        for (dimNum <- dimsNum to 1 by -1) {
          code.append(s"(${(itt + dimNum).toChar})")
        }
      }
      code.append(")")

      for (dimNum <- dimsNum to 1 by -1) {
        code.append(s"}\n new ${recordTypes(dimNum-1)}(d${dimNum})\n")
      }

      code.toString()
    }

    def addReadVoxelLoop(argType: Type): String = {
      argType match {
        case _ : ListType => {
          //          val valueIdx = new StringBuilder()

          if (dimsNum > 0) {
            val code = new StringBuilder()
            var itt = 'i'

            /*add loop if there are more than one values*/
            code.append(s"for(${(itt + dimsNum+1).toChar} <- 0 until ${valsNum}) yield {\n")

            for (dimNum <- dimsNum to 1 by -1) {
              code.append(s"for(${(itt + dimNum).toChar} <- 0 until ${dimsLength(dimNum - 1)}) yield {\n")
            }

            code.append(castNiftiTypeToRawType())
            code.append(List.fill(dimsNum+1)("}").mkString(""))

            code.toString()

          } else {
            castNiftiTypeToRawType()
          }
        }
        case _ => throw new  RuntimeException(s"Unexpected data type structure: $argType")
      }
    }

    def castNiftiTypeToRawType(): String = {
      val bytes = bitpix / 8
      val code = new StringBuilder()
      code.append(s"val bytes = super.getBytes(is, ${bytes})\n")
      dataType match {
        case NiftiDataTypes.NIFTI_TYPE_INT8 => {
          code.append("val data = super.getByte(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_UINT8 => {
          code.append("val data = super.getUByte(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_INT16 => {
          code.append("val data = super.getShort(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_UINT16 => {
          code.append("val data = super.getUShort(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_INT32 => {
          code.append("val data = super.getInt(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_UINT32 => {
          code.append("val data = super.getUInt(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_INT64 => {
          code.append("val data = super.getLong(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_FLOAT32 => {
          code.append("val data = super.getFloat(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n")
        }
        case NiftiDataTypes.DT_BINARY => {
          code.append("val data = super.getBoolean(bytes).toFloat\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_COMPLEX64 => {
          code.append("for(a <- 0 until 2) {\n")
          code.append("val data = super.getFloat(bytes)\n")
          code.append(s"(data*(${scale(0)})+(${scale(1)})).toFloat\n}\n")
        }
        case NiftiDataTypes.NIFTI_TYPE_RGB24 => {
          code.append("for(a <- 0 until 3) \n")
          code.append("val data = super.getUByte(bytes)\n")
        }
        case _ => throw new UnsupportedOperationException(s"Unexpected NIFTI field type: ${NiftiDataTypes.decode(dataType)}")
      }

      code.toString
    }

    build()

  }


}
