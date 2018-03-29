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

import raw.inputformats.InputFormat

trait BidsInputFormat extends InputFormat {
  final val formatId = "bids"

  protected final val NIFTI_FILENAMES = "NIFTI_FILENAMES"
  protected final val DIMS_NUM = "DIMS_NUM"
  protected final val DIMS_LENGTH = "DIMS_LENGTH"
  protected final val VALS_NUM = "VALS_NUM"
  protected final val VOX_OFFSET = "VOX_OFFSET"
  protected final val LITTLE_ENDIAN = "LITTLE_ENDIAN"
  protected final val DATATYPE = "DATATYPE"
  protected final val SCALE = "SCALE"
  protected final val BITPIX = "BITPIX"
  protected final val MAX_DIM_NUMS = "MAX_DIM_NUMS"
  protected final val MAX_VAL_NUMS ="MAX_VAL_NUMS"
  protected final val MAX_DIM_LENGTHS = "MAX_DIM_LENGTHS"
}
