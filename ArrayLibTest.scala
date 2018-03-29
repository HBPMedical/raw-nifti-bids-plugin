package raw.executor

class ArrayLibTest extends NewStyleExecutorTest {

  textData("array3D", "json",
    """[[[0.0, 1.0, 2.0],
      | [3.0, 4.0, 5.0],
      | [6.0, 7.0, 8.0]],
      |
      | [[9.0, 10.0, 11.0],
      | [12.0, 13.0, 14.0],
      | [15.0, 16.0, 17.0]],
      |
      | [[18.0, 19.0, 20.0],
      | [21.0, 22.0, 23.0],
      | [24.0, 25.0, 26.0]]]""".stripMargin)

  textData("array2D", "json",
    """[[0.0, 1.0, 2.0],
      | [3.0, 4.0, 5.0],
      | [6.0, 7.0, 8.0]]""".stripMargin)

  test(
    """
      |{
      |  a := [
      |   [[1.0,2.0],[3.0,4.0]],
      |   [[5.0,6.0],[7.0,8.0]]
      |  ];
      |
      |  xyz(a)
      |}
    """.stripMargin) { x =>
    x should evaluateTo(
      """[
        |   [[(x:0,y:0,z:0, value:1.0), (x:0,y:0,z:1, value:2.0)], [(x:0,y:1,z:0, value:3.0), (x:0,y:1,z:1, value:4.0)]],
        |   [[(x:1,y:0,z:0, value:5.0), (x:1,y:0,z:1, value:6.0)], [(x:1,y:1,z:0, value:7.0), (x:1,y:1,z:1, value:8.0)]]
        |  ]""".stripMargin)
  }

  test("slicex(array3D, 1)") { x =>
    x should evaluateTo(
      """[[9.0, 10.0, 11.0],
        | [12.0, 13.0, 14.0],
        | [15.0, 16.0, 17.0]]""".stripMargin)
  }

  test("slicey(array3D, 1)") { x =>
    x should evaluateTo(
      """[[3.0, 4.0, 5.0],
        | [12.0, 13.0, 14.0],
        | [21.0, 22.0, 23.0]]""".stripMargin)
  }

  test("slicez(array3D, 1)") { x =>
    x should evaluateTo(
      """[[1.0, 4.0, 7.0],
        | [10.0, 13.0, 16.0],
        | [19.0, 22.0, 25.0]]""".stripMargin)
  }

  test("crop2d(array2D, 1, 1, 2, 2)") { x =>
    x should evaluateTo(
      """[[4.0, 5.0],
        | [7.0, 8.0]]""".stripMargin)
  }

  test("crop2d(array2D, 0, 1, 3, 2)") { x =>
    x should evaluateTo(
      """[[1.0, 2.0],
        | [4.0, 5.0],
        | [7.0, 8.0]]""".stripMargin)
  }

  test("crop3d(array3D,2,1,0,1,2,3)") { x =>
    x should evaluateTo(
      """[[[21.0, 22.0, 23.0],
        |  [24.0, 25.0, 26.0]]]""".stripMargin)
  }

  test("crop3d(array3D,1,1,1,2,2,2)") { x =>
    x should evaluateTo(
      """[[[13.0, 14.0],
        |  [16.0, 17.0]],
        |
        |[[22.0, 23.0],
        | [25.0, 26.0]]]""".stripMargin)
  }

  test("transpose2d(array2D)") { x =>
    x should evaluateTo(
      """[[0.0, 3.0, 6.0],
        | [1.0, 4.0, 7.0],
        | [2.0, 5.0, 8.0]]""".stripMargin)
  }

  test("transpose3d(array3D)".stripMargin) { x =>
    x should evaluateTo(
      """[[[0.0, 1.0, 2.0],
        |  [9.0, 10.0, 11.0],
        |  [18.0, 19.0, 20.0]],
        |
        | [[3.0, 4.0, 5.0],
        |  [12.0, 13.0, 14.0],
        |  [21.0, 22.0, 23.0]],
        |
        | [[6.0, 7.0, 8.0],
        |  [15.0, 16.0, 17.0],
        |  [24.0, 25.0, 26.0]]]""".stripMargin)
  }

  // same as slicex(array3D, 1)
  test("sliceplane(array3D, 1, 1, 1, 1, 0, 0)") { x =>
    x should evaluateTo(
      """[[9.0, 10.0, 11.0],
        | [12.0, 13.0, 14.0],
        | [15.0, 16.0, 17.0]]""".stripMargin)
  }

  // same as slicey(array3D, 1)
  test("sliceplane(array3D, 1, 1, 1, 0, 1, 0)") { x =>
    x should evaluateTo(
      """[[3.0, 4.0, 5.0],
        | [12.0, 13.0, 14.0],
        | [21.0, 22.0, 23.0]]""".stripMargin)
  }

  // same as slicez(array3D, 1)
  test("sliceplane(array3D, 1, 1, 1, 0, 0, 1)") { x =>
    x should evaluateTo(
      """[[1.0, 4.0, 7.0],
        | [10.0, 13.0, 16.0],
        | [19.0, 22.0, 25.0]]""".stripMargin)
  }

  // diagonal over xy starting on 0
  test("sliceplane(array3D, 0, 0, 0, 1, -1, 0)") { x =>
    x should evaluateTo(
      """[[0.0, 1.0, 2.0],
        | [12.0, 13.0, 14.0],
        | [24.0, 25.0, 26.0]]""".stripMargin)
  }
}
