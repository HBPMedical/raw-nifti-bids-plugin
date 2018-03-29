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

import raw.inputformats.InputFormatDescriptor

object NiftiInputFormatDescriptor extends NiftiInputFormat {

  def apply(dimsNum: Short, dimsLength: Array[Short], valsNum: Short, vox_offset: Float, isLittleEndian: Boolean, dataType: Short, scale: Array[Float], bitpix: Short): InputFormatDescriptor = {
    InputFormatDescriptor(
      formatId,
      Map(
        DIMS_NUM -> dimsNum.toString,
        DIMS_LENGTH -> dimsLength.mkString(" "),
        VALS_NUM -> valsNum.toString,
        VOX_OFFSET -> vox_offset.toString,
        LITTLE_ENDIAN -> isLittleEndian.toString,
        DATATYPE -> dataType.toString,
        SCALE -> scale.mkString(" "),
        BITPIX -> bitpix.toString
      )
    )
  }

  def unapply(format: InputFormatDescriptor): Option[(Short, Array[Short], Short, Float, Boolean, Short, Array[Float], Short)] = {
    if (format.formatId != formatId) {
      None
    } else {
      Some(
        java.lang.Short.parseShort(format.options(DIMS_NUM)),
        format.options(DIMS_LENGTH).split(" ").map(_.toShort),
        java.lang.Short.parseShort(format.options(VALS_NUM)),
        java.lang.Float.parseFloat(format.options(VOX_OFFSET)),
        java.lang.Boolean.parseBoolean(format.options(LITTLE_ENDIAN)),
        java.lang.Short.parseShort(format.options(DATATYPE)),
        format.options(SCALE).split(" ").map(_.toFloat),
        java.lang.Short.parseShort(format.options(BITPIX))
      )
    }
  }
}
