package raw.inputformats.bids

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

import raw.inputformats.nifti.NiftiInputFormatDescriptor.{DIMS_LENGTH, DIMS_NUM}
import raw.inputformats.{Encoding, InputFormatDescriptor}



object BidsInputFormatDescriptor extends BidsInputFormat {

  def apply(niftiFileNames: Array[String], dimsNum: Array[Short], dimsLength: Array[Array[Short]], valsNum: Array[Short],
            vox_offset: Array[Float], isLittleEndian: Array[Boolean],
            dataType: Array[Short], scale: Array[Array[Float]], bitpix: Array[Short],
            maxDimNums: Short, maxValNums: Short, maxDimLengths: Array[Short]): InputFormatDescriptor = {

    InputFormatDescriptor(
      formatId,
      Map(
        NIFTI_FILENAMES -> niftiFileNames.mkString(";"),
        DIMS_NUM -> dimsNum.mkString(" "),
        DIMS_LENGTH ->  dimsLength.map(_.mkString(",")).mkString(" "),  //dimsLength.mkString(" ").mkString(","),
        VALS_NUM -> valsNum.mkString(" "),
        VOX_OFFSET -> vox_offset.mkString(" "),
        LITTLE_ENDIAN -> isLittleEndian.mkString(" "),
        DATATYPE -> dataType.mkString(" "),
        SCALE -> scale.map(_.mkString(",")).mkString(" "),
        BITPIX -> bitpix.mkString(" "),
        MAX_DIM_NUMS -> maxDimNums.toString,
        MAX_VAL_NUMS -> maxValNums.toString,
        MAX_DIM_LENGTHS -> maxDimLengths.mkString(" ")
      )
    )
  }

  def unapply(format: InputFormatDescriptor): Option[(Array[String], Array[Short], Array[Array[Short]], Array[Short],
                                                      Array[Float], Array[Boolean], Array[Short], Array[Array[Float]],
                                                      Array[Short], Short, Short, Array[Short])] =
  {
    if (format.formatId != formatId) {
      None
    } else {
      Some(
        format.options(NIFTI_FILENAMES).split(";").map(_.toString),
        format.options(DIMS_NUM).split(" ").map(_.toShort),
        format.options(DIMS_LENGTH).split(" ").map(_.split(",").map(_.toShort)),
        format.options(VALS_NUM).split(" ").map(_.toShort),
        format.options(VOX_OFFSET).split(" ").map(_.toFloat),
        format.options(LITTLE_ENDIAN).split(" ").map(_.toBoolean),
        format.options(DATATYPE).split(" ").map(_.toShort),
        format.options(SCALE).split(" ").map(_.split(",").map(_.toFloat)),
        format.options(BITPIX).split(" ").map(_.toShort),
        java.lang.Short.parseShort(format.options(MAX_DIM_NUMS)),
        java.lang.Short.parseShort(format.options(MAX_VAL_NUMS)),
        format.options(MAX_DIM_LENGTHS).split(" ").map(_.toShort)
      )
    }
  }
}
