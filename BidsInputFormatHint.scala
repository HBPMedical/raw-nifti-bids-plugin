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

import raw.inputformats.{Encoding, InputFormatHint}

import scala.collection.mutable

object BidsInputFormatHint extends BidsInputFormat {

  def apply(encoding: Option[Encoding] = None): InputFormatHint = {
    val hints = new mutable.HashMap[String, String]()
    //TODO: what needs to happen here??
    encoding.foreach(v => hints.put(DIMS_NUM, Encoding.toName(v)))
    new InputFormatHint(formatId, hints.toMap)
  }

  def unapply(format: InputFormatHint): Option[Option[Encoding]] = {
    if (format.formatId != formatId) {
      None
    } else {
      Some(format.hints.get(DIMS_NUM).map(Encoding.fromName))
    }
  }

}
