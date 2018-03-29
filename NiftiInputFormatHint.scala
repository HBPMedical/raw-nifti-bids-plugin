package raw.inputformats.nifti

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

import raw.inputformats.InputFormatHint

import scala.collection.mutable
import scala.util.Try

object NiftiInputFormatHint extends NiftiInputFormat {

  def apply(dimsNum: Option[Short] = None,
            dimsLength: Option[Array[Short]] = None,
            vox_offset: Option[Float] = None,
            isLittleEndian: Option[Boolean] = None,
            dataType: Option[Short] = None,
            scale: Option[Array[Float]] = None): InputFormatHint = {
    val hints = new mutable.HashMap[String, String]()
    dimsNum.foreach(v => hints.put(DIMS_NUM, dimsNum.toString))
    dimsLength.foreach(v => hints.put(DIMS_LENGTH, dimsLength.mkString(" ")))
    vox_offset.foreach(v => hints.put(VOX_OFFSET, vox_offset.toString))
    isLittleEndian.foreach(v => hints.put(LITTLE_ENDIAN, isLittleEndian.toString))
    dataType.foreach(v => hints.put(DATATYPE, dataType.toString))
    scale.foreach(v => hints.put(SCALE, scale.mkString(" ")))
    new InputFormatHint(formatId, hints.toMap)
  }

  def unapply(format: InputFormatHint): Option[(Option[Short], Option[Array[Short]], Option[Float], Option[Boolean], Option[Short], Option[Array[Float]])] = {
    if (format.formatId != formatId) {
      None
    } else {
      Some(
        format.hints.get(DIMS_NUM).map(java.lang.Short.parseShort),
        format.hints.get(DIMS_LENGTH).map(_.split(" ").map(_.toShort)),
        format.hints.get(VOX_OFFSET).map(java.lang.Float.parseFloat),
        format.hints.get(LITTLE_ENDIAN).map(le => Try(le.toBoolean).getOrElse(false)),
        format.hints.get(DATATYPE).map(java.lang.Short.parseShort),
        format.hints.get(SCALE).map(_.split(" ").map(_.toFloat))
      )
    }  }

}
