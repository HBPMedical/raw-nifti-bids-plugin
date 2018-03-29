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

import raw.compiler.sql.source._
import raw.inferrer.nifti.NiftiHeader





/*defines parameters and fields of the class*/
class BidsHeader(val tsv_file_name: String, val json_file_name: String, val participant_name: String, val nifti_file_name: String, val nifti_hdr: NiftiHeader) {
  val ANZ_HDR_SIZE = 348
  val EXT_KEY_SIZE = 8


  def getType: Vector[SqlAttrType] = {
    var vect: Vector[SqlAttrType] = nifti_hdr.getType
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

  def participant: String = {
    participant_name
  }

  def nifti_file: String = {
    nifti_file_name
  }

  def tsv_file: String = {
    tsv_file_name
  }

  def json_file: String = {
    json_file_name
  }

  private[this] var mapFromNameToDatatype: Map[String, SqlType] = {
    Map(
      "participant" -> SqlStringType(false),
      "nifti_file" -> SqlStringType(false),
      "tsv_file" -> SqlStringType(false),
      "json_file" -> SqlStringType(false)

    )
  }

  override def toString: String =
    s"BidsHeader"

  def canEqual(a: Any) = a.isInstanceOf[BidsHeader]

  override def equals(that: Any): Boolean =
    that match {
      case that: BidsHeader => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

}
