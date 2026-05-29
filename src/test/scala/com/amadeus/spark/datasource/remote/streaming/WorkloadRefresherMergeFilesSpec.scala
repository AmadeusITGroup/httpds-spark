package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.client.RemoteFile
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class WorkloadRefresherMergeFilesSpec extends AnyFunSpec with Matchers {

  private def file(name: String, timeMs: Long): RemoteFile =
    RemoteFile(name, new Timestamp(timeMs))

  describe("WorkloadRefresher.mergeFiles") {

    it("should keep the earliest fetchedAt when a file appears in both old and fresh") {
      val old   = Seq(file("a.log", 1000))
      val fresh = Seq(file("a.log", 2000))

      val result = WorkloadRefresher.mergeFiles(old, fresh)

      result.size shouldBe 1
      result.head.name shouldBe "a.log"
      result.head.fetchedAt.getTime shouldBe 1000
    }

    it("should drop old files that are no longer in the fresh list") {
      val old   = Seq(file("a.log", 1000), file("b.log", 1100))
      val fresh = Seq(file("a.log", 2000), file("c.log", 2100))

      val result = WorkloadRefresher.mergeFiles(old, fresh)

      result.map(_.name).toSet shouldBe Set("a.log", "c.log")
    }

    it("should add new files from the fresh list") {
      val old   = Seq(file("a.log", 1000))
      val fresh = Seq(file("a.log", 2000), file("b.log", 2100))

      val result = WorkloadRefresher.mergeFiles(old, fresh)

      result.map(_.name).toSet shouldBe Set("a.log", "b.log")
    }

    it("should return results sorted by (fetchedAt, name)") {
      val old   = Seq(file("b.log", 500), file("a.log", 1000))
      val fresh = Seq(file("a.log", 2000), file("b.log", 2100), file("c.log", 300))

      val result = WorkloadRefresher.mergeFiles(old, fresh)

      result.map(_.name) shouldBe Seq("c.log", "b.log", "a.log")
    }

    it("should handle empty old list") {
      val fresh = Seq(file("a.log", 1000), file("b.log", 2000))

      val result = WorkloadRefresher.mergeFiles(Seq.empty, fresh)

      result.map(_.name) shouldBe Seq("a.log", "b.log")
    }

    it("should handle empty fresh list") {
      val old = Seq(file("a.log", 1000), file("b.log", 2000))

      val result = WorkloadRefresher.mergeFiles(old, Seq.empty)

      result shouldBe empty
    }

    it("should handle both lists empty") {
      val result = WorkloadRefresher.mergeFiles(Seq.empty, Seq.empty)
      result shouldBe empty
    }
  }
}
