package raw.executor

import raw.utils.Interpolators._
import raw.location.LocalFile
import raw.utils.RawUtils



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

class BidsTest extends NewStyleExecutorTest {

  //val testBids1 = LocalFile(RawUtils.toPath("data/bids/ds001/dataset_description.json")).rawUri
  localFile("testBids1", "data/nifti/dataset_description.json")
  localFile("testJSON1", "data/movies.json")


  //test(s"""SELECT * FROM "$testBids1" """) { x =>
  /*test("""select * from testBids1 where participant="16" """) { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
  }*/

  test("""select * from testBids1 """) { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,2000))
  }

  /*test("""select * from testJSON1""") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
  }*/

}
