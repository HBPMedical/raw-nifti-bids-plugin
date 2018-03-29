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

import java.nio.ByteBuffer

import raw.compiler.sql.source._

object NiftiDataTypes {
  val DT_NONE = 0
  val DT_BINARY = 1
  val NIFTI_TYPE_UINT8 = 2
  val NIFTI_TYPE_INT16 = 4
  val NIFTI_TYPE_INT32 = 8
  val NIFTI_TYPE_FLOAT32 = 16
  val NIFTI_TYPE_COMPLEX64 = 32
  val NIFTI_TYPE_FLOAT64 = 64
  val NIFTI_TYPE_RGB24 = 128
  val DT_ALL = 255
  val NIFTI_TYPE_INT8 = 256
  val NIFTI_TYPE_UINT16 = 512
  val NIFTI_TYPE_UINT32 = 768
  val NIFTI_TYPE_INT64 = 1024
  val NIFTI_TYPE_UINT64 = 1280
  val NIFTI_TYPE_FLOAT128 = 1536
  val NIFTI_TYPE_COMPLEX128 = 1792
  val NIFTI_TYPE_COMPLEX256 = 2048
  val NIFTI_TYPE_RGBA32 = 2304

  def decode(dcode: Short): String = dcode match {
    case DT_NONE => "DT_NONE"
    case DT_BINARY => "DT_BINARY"
    case NIFTI_TYPE_UINT8 => "NIFTI_TYPE_UINT8"
    case NIFTI_TYPE_INT16 => "NIFTI_TYPE_INT16"
    case NIFTI_TYPE_INT32 => "NIFTI_TYPE_INT32"
    case NIFTI_TYPE_FLOAT32 => "NIFTI_TYPE_FLOAT32"
    case NIFTI_TYPE_COMPLEX64 => "NIFTI_TYPE_COMPLEX64"
    case NIFTI_TYPE_FLOAT64 => "NIFTI_TYPE_FLOAT64"
    case NIFTI_TYPE_RGB24 => "NIFTI_TYPE_RGB24"
    case DT_ALL => "DT_ALL"
    case NIFTI_TYPE_INT8 => "NIFTI_TYPE_INT8"
    case NIFTI_TYPE_UINT16 => "NIFTI_TYPE_UINT16"
    case NIFTI_TYPE_UINT32 => "NIFTI_TYPE_UINT32"
    case NIFTI_TYPE_INT64 => "NIFTI_TYPE_INT64"
    case NIFTI_TYPE_UINT64 => "NIFTI_TYPE_UINT64"
    case NIFTI_TYPE_FLOAT128 => "NIFTI_TYPE_FLOAT128"
    case NIFTI_TYPE_COMPLEX128 => "NIFTI_TYPE_COMPLEX128"
    case NIFTI_TYPE_COMPLEX256 => "NIFTI_TYPE_COMPLEX256"
    case NIFTI_TYPE_RGBA32 => "NIFTI_TYPE_RGBA32"
    case _ => "INVALID_NIFTI_DATATYPE_CODE"
  }
}

object NiftiUnits {
  val NIFTI_UNITS_UNKNOWN = 0
  val NIFTI_UNITS_METER = 1
  val NIFTI_UNITS_MM = 2
  val NIFTI_UNITS_MICRON = 3
  val NIFTI_UNITS_SEC = 8
  val NIFTI_UNITS_MSEC = 16
  val NIFTI_UNITS_USEC = 24
  val NIFTI_UNITS_HZ = 32
  val NIFTI_UNITS_PPM = 40
  val NIFTI_UNITS_RADS = 48

  def decode(dcode: Short): String = dcode match {
    case NIFTI_UNITS_UNKNOWN => "NIFTI_UNITS_UNKNOWN"
    case NIFTI_UNITS_METER => "NIFTI_UNITS_METER"
    case NIFTI_UNITS_MM => "NIFTI_UNITS_MM"
    case NIFTI_UNITS_MICRON => "NIFTI_UNITS_MICRON"
    case NIFTI_UNITS_SEC => "NIFTI_UNITS_SEC"
    case NIFTI_UNITS_MSEC => "NIFTI_UNITS_MSEC"
    case NIFTI_UNITS_USEC => "NIFTI_UNITS_USEC"
    case NIFTI_UNITS_HZ => "NIFTI_UNITS_HZ"
    case NIFTI_UNITS_PPM => "NIFTI_UNITS_PPM"
    case NIFTI_UNITS_RADS => "NIFTI_UNITS_RADS"
    case _ => "INVALID_NIFTI_UNITS_CODE"
  }

}

object QS_form_codes {
  val UNKNOWN = 0
  val SCANNER_ANAT = 1
  val ALIGNED_ANAT = 2
  val TALAIRACH = 3
  val MNI_152 = 5

  def decode(dcode: Short) : String = dcode match {
  case UNKNOWN => "UNKNOWN"
  case SCANNER_ANAT => "SCANNER_ANAT"
  case ALIGNED_ANAT => "ALIGNED_ANAT"
  case TALAIRACH => "TALAIRACH"
  case MNI_152 => "MNI_152"
  case _ => "INVALID_QS_form_code"
}
}

/*defines parameters and fields of the class*/
class NiftiHeader(val values: Array[Byte]) {
  val ANZ_HDR_SIZE = 348
  val EXT_KEY_SIZE = 8

  val littleEndian: Boolean = (dim0 < 1 || dim0 > 7)
  /*112-115*/
  /*I put it in val because I want to read from file only once no matter how many times I call the scl_scope or scl_inter*/
  val slope: Float = {
    if (littleEndian) ByteBuffer.wrap(values.slice(112, 116).reverse).getFloat
    else ByteBuffer.wrap(values.slice(112, 116)).getFloat /*112-115*/
  }

  /*used in infer and query*/
  private[this] val dim0 = ByteBuffer.wrap(values.slice(40, 42)).getShort
  var freq_dim: Int = _
  var phase_dim: Int = _
  var slice_dim: Int = _
  var extension: Array[Int] /*4*/ = _
  /*348-351*/
  var extensions_list: Seq[Seq[Int]] = _
  var extension_blobs: Seq[Seq[Int]] = _
  private[this] var mapFromNameToDatatype: Map[String, SqlType] = {
    Map(
      "xyz_unit_code" -> SqlByteType(false), // obtained from xyzt_units
      "t_unit_code" -> SqlByteType(false), // -> unit text codes

      "qfac" -> SqlIntType(false), // -1 or 1, stored in the otherwise unused pixdim[0]
      //
      //      AttrType("extensions_list", CollectionType(CollectionType(IntType()))), // How to give access
      //      AttrType("extension_blobs", CollectionType(CollectionType(IntType()))), // to header extensions ?

      // Original header fields

      "sizeof_hdr" -> SqlIntType(false), // MUST be 348
      "data_type" -> SqlStringType(false), // UNUSED
      "db_name" -> SqlStringType(false), // UNUSED
      "extents" -> SqlIntType(false), // UNUSED
      "session_error" -> SqlShortType(false), // UNUSED
      "regular" -> SqlByteType(false), // UNUSED
      "dim" -> SqlCollectionType(SqlShortType(false), false), // data array dimensions -> up to 8 short integers
      "intent" -> SqlCollectionType(SqlFloatType(false), false), // -> 3 intent parameters (
      "intent_code" -> SqlShortType(false), // NIFTI_INTENT_* code
      "intent_name" -> SqlStringType(false), // * text value corresponding to intent_code
      "datatype" -> SqlIntType(false), // defines data type!
      "datatype_name" -> SqlStringType(false), // defines data type!
      "bitpix" -> SqlShortType(false), // number of bits per voxel (cf. datatype)
      "pixdim" -> SqlCollectionType(SqlFloatType(false), false), // grid spacing
      "vox_offset" -> SqlFloatType(false), // offset into .nii file
      "scl_slope" -> SqlFloatType(false), // data scaling: slope
      "scl_inter" -> SqlFloatType(false), // data scaling: offset

      /** ***********************************************************************************
        * contain info about the timing of the fMRI acquisition and should be used together *
        * *************************************************************************************/
      "dim_info" -> SqlByteType(false), // MRI slice ordering
      "slice_code" -> SqlIntType(false),
      /*
       * 0 -> Slice order unknown
       * 1 -> Sequential, increasing
       * 2 -> Sequential, decreasing
       * 3 -> Interleaved, increasing, starting at the 1st mri slice
       * 4 -> Interleaved, decreasing, starting at the last mri slice
       * 5 -> Interleaved, increasing, starting at the 2nd mri slice
       * 6 -> Interleaved, decreasing, starting at one before the last mri slice
       */
      "slice_start" -> SqlShortType(false), // first slice index
      "slice_end" -> SqlIntType(false), // last slice index
      "slice_duration" -> SqlFloatType(false), // time needed to acquire 1 slice
      /** ***********************************************************************************/

      "xyzt_units" -> SqlIntType(false), // units of pixdim[1..4]
      "cal_max" -> SqlFloatType(false), // max display intensity
      "cal_min" -> SqlFloatType(false), // min display intensity
      "toffset" -> SqlFloatType(false), // time axis shift
      "glmax" -> SqlIntType(false), // UNUSED
      "glmin" -> SqlIntType(false), // UNUSED
      "descrip" -> SqlStringType(false), // any text you like
      "aux_file" -> SqlStringType(false), // auxiliary filename
      "qform_code" -> SqlShortType(false), // NIFTI_XFORM_* code
      "sform_code" -> SqlShortType(false), // NIFTI_XFORM_* code
      "quaterns" -> SqlCollectionType(SqlFloatType(false), false), // quaternion b,c,d parameters
      "qoffsets" -> SqlCollectionType(SqlFloatType(false), false), // quaternion x,y,z shift
      "srow_x" -> SqlCollectionType(SqlFloatType(false), false), // 1st row affine transform
      "srow_y" -> SqlCollectionType(SqlFloatType(false), false), // 2nd row affine transform
      "srow_z" -> SqlCollectionType(SqlFloatType(false), false) // 3rd row affine transform
    )
  }

  /*used only in query*/

  /*40-55*/
  //dim[0] is the number of dimensions, dim[i>0] is the number of elements (voxels) in each dimension
  def dim: Array[Short] = {
    if (littleEndian) values.slice(40, 56).grouped(2).toArray.map(_.reverse).map(ByteBuffer.wrap(_).getShort)
    else values.slice(40, 56).sliding(2, 2).toArray.map(ByteBuffer.wrap(_).getShort)
  }

  def datatype_name: String = NiftiDataTypes.decode(datatype)

  /*70-71*/
  def datatype: Short = {
    if (littleEndian) ByteBuffer.wrap(values.slice(70, 72).reverse).getShort
    else ByteBuffer.wrap(values.slice(70, 72)).getShort
  }

  /*72-73*/
  //the number of bits per voxel
  def bitpix: Short = {
    if (littleEndian) ByteBuffer.wrap(values.slice(72, 74).reverse).getShort
    else ByteBuffer.wrap(values.slice(72, 74)).getShort
  }

  /*108-111*/
  def vox_offset: Float = {
    if (littleEndian) ByteBuffer.wrap(values.slice(108, 112).reverse).getFloat
    else ByteBuffer.wrap(values.slice(108, 112)).getFloat
  }

  def scl_slope: Float = if (slope == 0) 1 else slope

  /*116-119*/
  def scl_inter: Float = {
    if (slope == 0) 0
    else if (littleEndian) ByteBuffer.wrap(values.slice(116, 120).reverse).getFloat
    else ByteBuffer.wrap(values.slice(116, 120)).getFloat /*116-119*/
  }

  /*4-13*/
  def data_type: String = values.slice(4, 14).map(_.toChar).mkString.trim

  /*14-31*/
  def db_name: String = values.slice(14, 32).map(_.toChar).mkString.trim

  /*32-35*/
  def extents: Int = {
    if (littleEndian) ByteBuffer.wrap(values.slice(32, 36).reverse).getInt
    else ByteBuffer.wrap(values.slice(32, 36)).getInt /*32-35*/
  }

  /*36-37*/
  def session_error: Short = {
    if (littleEndian) ByteBuffer.wrap(values.slice(36, 38).reverse).getShort
    else ByteBuffer.wrap(values.slice(36, 38)).getShort /*36-37*/
  }

  /*38*/
  def regular: Byte = values.slice(38, 39).head

  /*39*/
  def dim_info: Byte = values.slice(39, 40).head

  /*56-67*/
  def intent: Array[Float] /*3*/ = {
    if (littleEndian) values.slice(56, 68).grouped(4).toArray.map(_.reverse).map(ByteBuffer.wrap(_).getFloat)
    else values.slice(56, 68).sliding(4, 4).toArray.map(ByteBuffer.wrap(_).getFloat) /*56-67*/
  }

  /*68-69*/
  def intent_code: Short = {
    if (littleEndian) ByteBuffer.wrap(values.slice(68, 70).reverse).getShort
    else ByteBuffer.wrap(values.slice(68, 70)).getShort /*68-69*/
  }

  /*74-75*/
  def slice_start: Short = {
    if (littleEndian) ByteBuffer.wrap(values.slice(74, 76).reverse).getShort
    else ByteBuffer.wrap(values.slice(74, 76)).getShort

  }

  /*76-107*/
  //pixdim[0] should always be -1 or 1 when reading a NIFTI-1 header
  //pixdim[i>0] is the width of each voxel in dimension i
  def pixdim: Array[Float] /*8*/ = {
    if (littleEndian) values.slice(76, 108).grouped(4).toArray.map(_.reverse).map(ByteBuffer.wrap(_).getFloat)
    else values.slice(76, 108).sliding(4, 4).toArray.map(ByteBuffer.wrap(_).getFloat)
  }

  def qfac: Int = {
    val pd : Int = pixdim(0).toInt
    if(pd != -1 && pd != 1) 1
    else pd
  }


  /*120-121*/
  def slice_end: Short = {
    if (littleEndian) ByteBuffer.wrap(values.slice(120, 122).reverse).getShort
    else ByteBuffer.wrap(values.slice(120, 122)).getShort /*120-121*/
  }

  /*122*/
  def slice_code: Byte = values.slice(122, 123).head

  //the measurement units of each voxel, i.e does the voxel represents meters, millisecods, etc
  def xyz_unit_code: Byte = (xyzt_units & 0x07).toByte //get the units for dimensions 1,2,3 (the spatial dimensions)

  /*123*/
  def xyzt_units: Byte = values.slice(123, 124).head

  def t_unit_code: Byte = (xyzt_units & 0x38).toByte //get the units for dimension 4 (the temporal dimension)

  /*124-127*/
  def cal_max: Float = {
    if (littleEndian) ByteBuffer.wrap(values.slice(124, 128).reverse).getFloat
    else ByteBuffer.wrap(values.slice(124, 128)).getFloat /*124-127*/
  }

  /*128-131*/
  def cal_min: Float = {
    if (littleEndian) ByteBuffer.wrap(values.slice(128, 132).reverse).getFloat
    else ByteBuffer.wrap(values.slice(128, 132)).getFloat /*128-131*/
  }

  /*132-135*/
  def slice_duration: Float = {
    if (littleEndian) ByteBuffer.wrap(values.slice(132, 136).reverse).getFloat
    else ByteBuffer.wrap(values.slice(132, 136)).getFloat
  }

  /*136-139*/
  def toffset: Float = {
    if (littleEndian) ByteBuffer.wrap(values.slice(136, 140).reverse).getFloat
    else ByteBuffer.wrap(values.slice(136, 140)).getFloat
  }

  /*140-143*/
  def glmax: Int = {
    if (littleEndian) ByteBuffer.wrap(values.slice(140, 144).reverse).getInt
    else ByteBuffer.wrap(values.slice(140, 144)).getInt
  }

  /*144-147*/
  def glmin: Int = {
    if (littleEndian) ByteBuffer.wrap(values.slice(144, 148).reverse).getInt
    else ByteBuffer.wrap(values.slice(144, 148)).getInt
  }

  /*148-227*/
  def descrip: String = values.slice(148, 228).map(_.toChar).mkString.trim

  /*228-251*/
  def aux_file: String = values.slice(228, 252).map(_.toChar).mkString.trim

  /*252-253*/
  def qform_code: Short = {
    if (littleEndian) ByteBuffer.wrap(values.slice(252, 254).reverse).getShort
    else ByteBuffer.wrap(values.slice(252, 254)).getShort
  }

  /*254-255*/
  def sform_code: Short = {
    if (littleEndian) ByteBuffer.wrap(values.slice(254, 256).reverse).getShort
    else ByteBuffer.wrap(values.slice(254, 256)).getShort
  }

  /*256-267*/
  def quaterns: Array[Float] /*3*/ = {
    if (littleEndian) values.slice(256, 268).grouped(4).toArray.map(_.reverse).map(ByteBuffer.wrap(_).getFloat)
    else values.slice(256, 268).sliding(4, 4).toArray.map(ByteBuffer.wrap(_).getFloat)
  }

  /*268-279*/
  def qoffsets: Array[Float] /*3*/ = {
    if (littleEndian) values.slice(268, 280).grouped(4).toArray.map(_.reverse).map(ByteBuffer.wrap(_).getFloat)
    else values.slice(268, 280).sliding(4, 4).toArray.map(ByteBuffer.wrap(_).getFloat)
  }

  /*280-327*/
  def srow_x: Array[Float] /*4*/ = {
    if (littleEndian) values.slice(280, 296).grouped(4).toArray.map(_.reverse).map(ByteBuffer.wrap(_).getFloat)
    else values.slice(280, 296).sliding(4, 4).toArray.map(ByteBuffer.wrap(_).getFloat)
  }

  def srow_y: Array[Float] /*4*/ = {
    if (littleEndian) values.slice(296, 312).grouped(4).toArray.map(_.reverse).map(ByteBuffer.wrap(_).getFloat)
    else values.slice(296, 312).sliding(4, 4).toArray.map(ByteBuffer.wrap(_).getFloat)
  }

  def srow_z: Array[Float] /*4*/ = {
    if (littleEndian) values.slice(312, 328).grouped(4).toArray.map(_.reverse).map(ByteBuffer.wrap(_).getFloat)
    else values.slice(312, 328).sliding(4, 4).toArray.map(ByteBuffer.wrap(_).getFloat)
  }

  /*328-343*/
  def intent_name: String = values.slice(328, 344).map(_.toChar).mkString.trim

  /*344-347*/
  def magic: String = values.slice(344, 348).map(_.toChar).mkString.trim

  /*
  /*the DataInput is correct depending on the little-endian, i.e. it is DataInputStream or LittleEndianDataInputStream*/
  def this(input: DataInput) {
    dim_info = input.readUnsignedByte.toChar
    freq_dim = (dim_info.toInt & 3)
    phase_dim = ((dim_info.toInt >>> 2) & 3)
    slice_dim = ((dim_info.toInt >>> 4) & 3)
    for (i <- 0 until 8) pixdim(i) = input.readFloat
    qfac = Math.floor(pixdim(0).toDouble).toInt
    var extBytes = new Array[Byte](4) map (_ => 0: Byte)
    input.readFully(extBytes, 0, 4)
    extension = extBytes map { case a: Byte => a.toInt }
    if (extension(0) != (0: Byte)) {
      extension_blobs = List()
      var start_addr = ANZ_HDR_SIZE + 4
      while (start_addr < vox_offset.toInt) {
        val size_code = new Array[Int](2)
        size_code(0) = input.readInt
        size_code(1) = input.readInt
        val nb = size_code(0) - EXT_KEY_SIZE
        val eblob = new Array[Byte](nb)
        input.readFully(eblob)
        val seqBlob: Seq[Int] = eblob.toList map { case a: Byte => a.toInt }
        val seqCode: Seq[Int] = size_code.toList

        extension_blobs = seqBlob +: extension_blobs
        extensions_list = seqCode +: extensions_list
        start_addr += size_code(0)
        if (start_addr > vox_offset.toInt) throw new PublicException("Error: Data  for extension " + extensions_list.size + " appears to overrun start of image data.")
      }
    }
  }
*/
  def getType: Vector[SqlAttrType] = {
    var vect: Vector[SqlAttrType] = Vector.empty
    mapFromNameToDatatype.foreach(p => {
      vect = vect :+ SqlAttrType(p._1, p._2)
    })
    vect
  }

  def attrsNames: List[String] = {
    var l: List[String] = List.empty
    mapFromNameToDatatype.foreach(p => {
      l = p._1 :: l
    })
    l
  }

  override def toString: String =
    s"($sizeof_hdr)"

  override def equals(that: Any): Boolean =
    that match {
      case that: NiftiHeader => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  def canEqual(a: Any) = a.isInstanceOf[NiftiHeader]

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + sizeof_hdr;
    return result
  }

  /*0-3*/
  def sizeof_hdr: Int = {
    if (littleEndian) ByteBuffer.wrap(values.slice(0, 4).reverse).getInt
    else ByteBuffer.wrap(values.slice(0, 4)).getInt
  }


}
