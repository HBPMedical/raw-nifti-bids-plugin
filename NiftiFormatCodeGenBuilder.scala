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

import raw.compiler.base.source.Type
import raw.compiler.common.source.FunAbs
import raw.compiler.core.source.RecordType
import raw.executor.spark.CodeGenContext
import raw.inputformats.InputFormatDescriptor
import raw.inputformats.nifti.NiftiInputFormatDescriptor

object NiftiFormatCodeGenBuilder {

  private val ai = new AtomicInteger()

  private[this] def hasHeader(projectedType: RecordType) = projectedType.atts.count(_.idn == "data") < projectedType.atts.length

  def buildHadoopCode(df: InputFormatDescriptor, projectedType: Type, fullType: Type,
                      maybeColumns: Option[Vector[String]], maybePredicate: Option[FunAbs])
                     (implicit codeGenContext: CodeGenContext): String = {
    assert(maybePredicate.isEmpty, "unexpected predicate push down")
    codeGenContext.addImport("raw.runtime.inputformats._")
    codeGenContext.addImport("raw.runtime.inputformats.nifti._")
    codeGenContext.addImport("raw.inferrer.nifti.NiftiHeader")
    codeGenContext.addImport("java.io.InputStream")
    codeGenContext.addImport("java.nio.ByteBuffer")


    codeGenContext.getOrElseGenerateAndUpdate(df.formatId, ("hadoop", df, projectedType, fullType),
      buildHadoopCode(df, projectedType, fullType))
  }

  private[this] def buildHadoopCode(df: InputFormatDescriptor, projectedType: Type, fullType: Type)
                                   (implicit codeGenContext: CodeGenContext): String = {
    val NiftiInputFormatDescriptor(dimsNum, dimsLength, valsNum, vox_offset, isLittleEndian, dataType, scale, bitpix) = df
    val classAndCode = NiftiRecordParserBuilder(df, projectedType.asInstanceOf[RecordType], hasHeader(projectedType.asInstanceOf[RecordType]))
    val recordParserClassName = classAndCode.className

    // create an input format name
    val id = NiftiFormatCodeGenBuilder.ai.getAndIncrement()
    val inputFormatTypeName = "NiftiInputFormat" + id
    val scalaTypeName = codeGenContext.buildType(projectedType)
    val codeGenBlock =
      s"""class $inputFormatTypeName extends RawInputFormat[$scalaTypeName](false) {
         |  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, $scalaTypeName] = {
         |    val recordParser = new ${recordParserClassName}(${isLittleEndian}, ${hasHeader(projectedType.asInstanceOf[RecordType])}, ${vox_offset}f)
         |    new NiftiReader(recordParser)
         |  }
         |}
         |""".stripMargin

    codeGenContext.addClass(inputFormatTypeName, codeGenBlock)

    s"""new HadoopScanner(classOf[$inputFormatTypeName], classOf[$scalaTypeName])"""
  }

  def buildDropboxCode(df: InputFormatDescriptor, projectedType: Type, fullType: Type,
                       maybeColumns: Option[Vector[String]], maybePredicate: Option[FunAbs])
                      (implicit codeGenContext: CodeGenContext): String = {
    assert(maybePredicate.isEmpty, "unexpected predicate push down")
    codeGenContext.addImport("raw.runtime.inputformats._")
    codeGenContext.addImport("raw.runtime.inputformats.nifti._")
    codeGenContext.addImport("java.io.DataInput")
    codeGenContext.getOrElseGenerateAndUpdate(df.formatId, ("dropbox", df, projectedType, fullType),
      buildDropboxCode(df, projectedType, fullType))
  }

  private[this] def buildDropboxCode(df: InputFormatDescriptor, projectedType: Type, fullType: Type)
                                    (implicit codeGenContext: CodeGenContext): String = {

    val classAndCode = NiftiRecordParserBuilder(df, projectedType.asInstanceOf[RecordType], hasHeader(projectedType.asInstanceOf[RecordType]))
    val recordParserClassName = classAndCode.className


    // create an input format name
    val id = NiftiFormatCodeGenBuilder.ai.getAndIncrement()
    val inputFormatTypeName = "NiftiInputFormat" + id
    val scalaTypeName = codeGenContext.buildType(projectedType)
    val codeGenBlock =
      s"""class $inputFormatTypeName extends RawInputFormat[$scalaTypeName](false) {
         |  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, $scalaTypeName] = {
         |    val recordParser = new ${recordParserClassName}()
         |    new NiftiReader(recordParser)
         |  }
         |}
         |""".stripMargin

    codeGenContext.addClass(inputFormatTypeName, codeGenBlock)

    s"""new DropboxScanner(classOf[$inputFormatTypeName], classOf[$scalaTypeName])"""
  }

  def buildHttpCode(df: InputFormatDescriptor, projectedType: Type, fullType: Type,
                    maybeColumns: Option[Vector[String]], maybePredicate: Option[FunAbs])
                   (implicit codeGenContext: CodeGenContext): String = {
    assert(maybePredicate.isEmpty, "unexpected predicate push down")
    codeGenContext.addImport("raw.runtime.inputformats._")
    codeGenContext.addImport("raw.runtime.inputformats.nifti._")
    codeGenContext.addImport("java.io.DataInput")
    codeGenContext.getOrElseGenerateAndUpdate(df.formatId, ("http", df, projectedType, fullType),
      buildHttpCode(df, projectedType, fullType))
  }

  private[this] def buildHttpCode(df: InputFormatDescriptor, projectedType: Type, fullType: Type)
                                 (implicit codeGenContext: CodeGenContext): String = {

    val classAndCode = NiftiRecordParserBuilder(df, projectedType.asInstanceOf[RecordType], hasHeader(projectedType.asInstanceOf[RecordType]))
    val recordParserClassName = classAndCode.className


    // create an input format name
    val id = NiftiFormatCodeGenBuilder.ai.getAndIncrement()
    val inputFormatTypeName = "NiftiInputFormat" + id
    val scalaTypeName = codeGenContext.buildType(projectedType)
    val codeGenBlock =
      s"""class $inputFormatTypeName extends RawInputFormat[$scalaTypeName](false) {
         |  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, $scalaTypeName] = {
         |    val recordParser = new ${recordParserClassName}()
         |    new NiftiReader(recordParser)
         |  }
         |}
         |""".stripMargin

    codeGenContext.addClass(inputFormatTypeName, codeGenBlock)

    s"""new HadoopScanner(classOf[$inputFormatTypeName], classOf[$scalaTypeName])"""
  }
}
