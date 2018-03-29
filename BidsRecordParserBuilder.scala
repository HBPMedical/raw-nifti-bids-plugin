package raw.executor.spark.inputformats.bids

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

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import raw.compiler.L0.source.ListType
import raw.compiler.base.source._
import raw.compiler.core.source.RecordType
import raw.executor.spark.{CodeGenClassCode, CodeGenContext}
import raw.inferrer.nifti.NiftiDataTypes
import raw.inputformats.InputFormatDescriptor
import raw.inputformats.bids.BidsInputFormatDescriptor

import scala.language.implicitConversions

object BidsRecordParserBuilder extends StrictLogging {

      private[this] val ai = new AtomicInteger()

      def apply(df: InputFormatDescriptor, projectedType: RecordType, hasHeader: Boolean)(implicit codeGenContext: CodeGenContext): CodeGenClassCode = {

          val BidsInputFormatDescriptor(niftiFileNames, dimsNum, dimsLength, valsNum, vox_offset, isLittleEndian, dataType, scale, bitpix, maxDimNums, maxValNums, maxDimLengths) = df

          def build(): CodeGenClassCode = {
            val scalaType = codeGenContext.buildType(projectedType)

            val args = new Array[String](projectedType.atts.length)
            var totalFields = 0
            val sourceCode = new StringBuilder()

            val getFormattedDataSourceCode = new StringBuilder()

            for (totalFields <- 0 until projectedType.atts.length) {
              val att = projectedType.atts(totalFields)
              args(totalFields) = "_" + totalFields
              att.idn match {
                case "data" => {
                    getFormattedDataSourceCode.append(generateFormattedData(att.tipe));

                    sourceCode.append(s"val _${totalFields} = ")
                    sourceCode.append("""Seq({getFormattedData( "" )})""")
                    sourceCode.append(s"\n")
                }
                case "participant" => {
                    sourceCode.append(s"val _${totalFields} = hdr.${att.idn}\n")
                }
                case "nifti_file" => {
                    sourceCode.append(s"val _${totalFields} = hdr.${att.idn}\n")
                }
                case "json_file" => {
                    sourceCode.append(s"val _${totalFields} = hdr.${att.idn}\n")
                }
                case "tsv_file" => {
                    sourceCode.append(s"val _${totalFields} = hdr.${att.idn}\n")
                }
                case _ => {
                    sourceCode.append(s"val _${totalFields} = nifti_hdr.${att.idn}\n")
                }
              }
            }
            sourceCode.append(s"\nnew ${codeGenContext.buildType(projectedType)}(${args.mkString(", ")})")

            val className = "BidsParser" + ai.getAndIncrement()

            val code = new StringBuilder()
            code.append(
                s"""class $className()
           |extends BidsParser[$scalaType]() {\n
           |override def parse(is: InputStream, tsv_file: String, json_file: String, participant: String, nifti_file: String, data:Array[Byte]): $scalaType = {\n""")
            if (hasHeader) {
                code.append(s"val nifti_hdr = new NiftiHeader(data)\n")

                code.append(s"val hdr = new BidsHeader(tsv_file, json_file, participant, nifti_file, nifti_hdr)\n")
            }
            code.append(s"${sourceCode.toString()}\n}")

            code.append(getFormattedDataSourceCode.toString())

            code.append(s"\n}\n")

            codeGenContext.addClass(className, code.toString())
            CodeGenClassCode(className, code.toString())
          }

          /*the compiler will use this to convert short+1 to short instead of int*/
        implicit def toShort(x: Int): Short = x.toShort

          /* Generate code to read the voxel data in the right order and return the expected type */

        def codeGenDataBuilder(): String = {
            ""
        }

        def addTransposeLoop(it: Char, dim: Short): String = {
            ""
        }


        def castBidsTypeToRawType_mine(valueIdx: String): String = {
            val code = new StringBuilder()
            val vIdn = s"input"
            code.append(s"is.read(data_input)\n") //read X bytes, store them in input starting from input[0]
            vIdn
          }

        def castBidsTypeToRawType(): String = {
            val vIdn = s"input"

            vIdn
        }

        /* Generate code to read the voxel data in the right order and return the expected type */
        def getNiftiTypes(record: Type) : String = {
          record match {
            case ListType(RecordType(vt)) => {
              codeGenContext.buildType(RecordType(vt))+","+getNiftiTypes(vt(0).tipe)
            }
            case _ => ""
          }
        }

        def generateFormattedData(record: Type) : String = {
          val dataSourceCode = new StringBuilder()
          val recordTypes: Array[String] = getNiftiTypes(record).split(",")

          dataSourceCode.append("\n\n")
          dataSourceCode.append(s"def getFormattedData(nifti_file: String) : ${recordTypes(0)} = { \n\n")


          dataSourceCode.append(s"nifti_file match {\n")

          //The default case for non-nifti files.
          //
          dataSourceCode.append("""case "" =>  """)
          dataSourceCode.append("\n\n")
          val itt = 'i'
          for (dimNum <- 1 to maxDimNums) {
            var dimsLengthI = 1;
            dataSourceCode.append(s"val d${dimNum} = for(${(itt + dimNum).toChar} <- 0 until ${dimsLengthI}) yield {\n")
          }
          dataSourceCode.append(s"new ${recordTypes(maxDimNums)}((0.0).toFloat")
          var j = 0
          for (j <- 1 until maxValNums) {
            dataSourceCode.append(", (0.0).toFloat")
          }
          dataSourceCode.append(")")
          for (dimNum <- maxDimNums to 1 by -1) {
            dataSourceCode.append(s"\n}\n new ${recordTypes(dimNum - 1)}(d${dimNum})\n")
          }

          System.out.println("niftiFileNames.length: " + niftiFileNames.length)
          System.out.println("valsNum.length: " + valsNum.length)
          // The other cases for nifti files.
          //
          for (i <- 0 until  niftiFileNames.length) {
            if (niftiFileNames(i) != "") {

              dataSourceCode.append("\n\ncase \"" +   niftiFileNames(i).replace("file:/", "file:///") + "\" =>\n\n");

              dataSourceCode.append(s"var is: InputStream = super.getInputStream(nifti_file)\n")
              dataSourceCode.append(s"val data: Array[Byte] = super.getBytes(is, 348)\n")

              dataSourceCode.append(s"val nifti_hdr = new NiftiHeader(data)\n")
              dataSourceCode.append(
                """
                is.skip(nifti_hdr.vox_offset.toInt - 348)
            """.stripMargin)

              val itt = 'i'

              dataSourceCode.append("val asReadDataBuffer = ArrayBuffer.fill(");
              dataSourceCode.append(s"${valsNum(i)}")
              for (dimNum <- dimsNum(i) to 1 by -1) {
                dataSourceCode.append(s",${dimsLength(i)(dimNum - 1)}\n")
              }
              dataSourceCode.append(s")((0.0).toFloat)\n")


              if (dimsNum(i) > 0) {
                val code = new StringBuilder()
                var itt = 'i'

                //add loop if there are more than one values
                dataSourceCode.append(s"for(${(itt + dimsNum(i) + 1).toChar} <- 0 until ${valsNum(i)})  {\n")

                for (dimNum <- dimsNum(i) to 1 by -1) {
                  dataSourceCode.append(s"for(${(itt + dimNum).toChar} <- 0 until ${dimsLength(i)(dimNum - 1)})  {\n")
                }

                dataSourceCode.append(castNiftiTypeToRawType(i))
                dataSourceCode.append(s"asReadDataBuffer")
                for (dimNum <- dimsNum(i)+1 to 1 by -1) {
                  dataSourceCode.append(s"(${(itt + dimNum).toChar})")
                }
                dataSourceCode.append(s" = floatValue")
                dataSourceCode.append(List.fill(dimsNum(i) + 1)("\n}").mkString(""))

                code.toString()

              } else {
                castNiftiTypeToRawType(i)
              }

              dataSourceCode.append(s"\nis.close()\n\n")
              dataSourceCode.append(s"\nval asReadData = asReadDataBuffer.toArray\n")

              /// Create the UserRecord object using asReadData

              for (dimNum <- 1 to maxDimNums) {
                var dimsLengthI = 1;
                if (dimsLength(i).length > dimNum - 1 ) {
                  dimsLengthI = dimsLength(i)(dimNum - 1)
                }
                dataSourceCode.append(s"val d${dimNum} = for(${(itt + dimNum).toChar} <- 0 until ${dimsLengthI}) yield {\n")
              }
              dataSourceCode.append(s"new ${recordTypes(maxDimNums)}(asReadData")
              dataSourceCode.append("(0)")

              for (dimNum <- dimsNum(i) to 1 by -1) {
                dataSourceCode.append(s"(${(itt + dimNum).toChar})")
              }

              var j = 0
              for (j <- 1 until maxValNums) {
                if (j < valsNum(i)) {
                  dataSourceCode.append(", asReadData")
                  dataSourceCode.append(s"(${j})")
                  for (dimNum <- dimsNum(i) to 1 by -1) {
                    dataSourceCode.append(s"(${(itt + dimNum).toChar})")
                  }
                }
                else {
                  dataSourceCode.append(", (0.0).toFloat")
                }
              }
              dataSourceCode.append(")")

              for (dimNum <- maxDimNums to 1 by -1) {
                dataSourceCode.append(s"\n}\n new ${recordTypes(dimNum - 1)}(d${dimNum})\n")
              }
            }
          }

          dataSourceCode.append(s"case _ =>\n")
          dataSourceCode.append("  throw new PublicException(\"Could not find matching nifti file:\" + nifti_file)\n")
          dataSourceCode.append("}\n")


          dataSourceCode.append(s"}\n\n")
          dataSourceCode.result()
        }


        def castNiftiTypeToRawType(i: Integer): String = {
          val bytes = bitpix(i) / 8
          val code = new StringBuilder()
          code.append(s"lazy val bytes = super.getBytes(is, ${bytes})\n")
          dataType(i) match {
            case NiftiDataTypes.NIFTI_TYPE_INT8 => {
              code.append("lazy val data = super.getByte(bytes)\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_UINT8 => {
              code.append("lazy val data = super.getUByte(bytes)\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_INT16 => {
              code.append(s"lazy val data = super.getShort(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_UINT16 => {
              code.append(s"lazy val data = super.getUShort(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_INT32 => {
              code.append(s"lazy val data = super.getInt(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_UINT32 => {
              code.append(s"lazy val data = super.getUInt(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_INT64 => {
              code.append(s"lazy val data = super.getLong(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_UINT64 => {
              code.append(s"lazy val data = super.getULong(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data.toFloat * (${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_FLOAT32 => {
              code.append(s"lazy val data = super.getFloat(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_FLOAT64 => {
              code.append(s"lazy val data = super.getDouble(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data.toFloat * (${scale(i)(0)})+(${scale(i)(1)})).toFloat\n")
            }
            case NiftiDataTypes.DT_BINARY => {
              code.append("lazy val floatValue =  = super.getBoolean(bytes).toFloat\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_COMPLEX64 => {
              code.append("for(a <- 0 until 2) {\n")
              code.append(s"lazy val data = super.getFloat(bytes, ${isLittleEndian(i)})\n")
              code.append(s"lazy val floatValue = (data*(${scale(i)(0)})+(${scale(i)(1)})).toFloat\n}\n")
            }
            case NiftiDataTypes.NIFTI_TYPE_RGB24 => {
              code.append("for(a <- 0 until 3) \n")
              code.append("lazy val floatValue = super.getUByte(bytes)\n")
            }
            case _ => throw new UnsupportedOperationException(s"Unexpected NIFTI field type: ${NiftiDataTypes.decode(dataType(i))}")
          }

          code.toString
        }

        build()

      }


  }
