package com.amadeus.spark.datasource.remote.helpers

import com.amadeus.spark.datasource.remote.RemoteFileInputPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PartitionsHelperTest extends AnyFunSpec with Matchers {

  /** Minimal concrete implementation to expose the protected method */
  object TestPartitionsHelper extends PartitionsHelper {
    def partitions(numPartitions: Int, fileNames: Seq[String]): Array[RemoteFileInputPartition] =
      createPartitions(numPartitions, fileNames)
  }

  describe("createPartitions()") {

    it("distributes files across partitions in round-robin order after sorting") {
      val result = TestPartitionsHelper.partitions(3, Seq("c.log", "a.log", "b.log", "d.log"))

      val expected = Seq(
        RemoteFileInputPartition(Seq("a.log", "d.log")),
        RemoteFileInputPartition(Seq("b.log")),
        RemoteFileInputPartition(Seq("c.log"))
      )

      result shouldEqual expected
    }

    it("distributes files evenly when the count is a multiple of numPartitions") {
      val files  = Seq("f1.log", "f2.log", "f3.log", "f4.log")
      val result = TestPartitionsHelper.partitions(2, files)

      val expected = Seq(
        RemoteFileInputPartition(Seq("f1.log", "f3.log")),
        RemoteFileInputPartition(Seq("f2.log", "f4.log"))
      )

      result shouldEqual expected
    }

    it("returns empty partitions when given no files") {
      val result = TestPartitionsHelper.partitions(3, Seq.empty)

      val expected = Seq(
        RemoteFileInputPartition(Seq.empty),
        RemoteFileInputPartition(Seq.empty),
        RemoteFileInputPartition(Seq.empty)
      )

      result shouldEqual expected
    }
  }
}
