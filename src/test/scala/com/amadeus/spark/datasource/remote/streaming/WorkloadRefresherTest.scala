package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class WorkloadRefresherTest extends AnyFunSpec with Matchers {

  private def createConfig(pollInterval: String = "0 seconds"): RemoteFileDataSourceOptions =
    RemoteFileDataSourceOptions.fromMap(
      new CaseInsensitiveStringMap(
        Map(
          "uri"          -> "http://localhost:5000",
          "remoteClient" -> "mock",
          "pollInterval" -> pollInterval
        ).asJava
      )
    )

  describe("refreshWorkload") {

    it("should return unprocessed files up to maxFiles") {
      val config    = createConfig()
      val refresher = new WorkloadRefresher(config)

      val workload = refresher.refreshWorkload(Set.empty, maxFiles = 2)

      val expectedFilesToProcess = Seq("file1.log", "file2.log")
      val expectedFullFileList   = Set("file1.log", "file2.log")

      workload.filesToProcess shouldEqual expectedFilesToProcess
      workload.fullFileList shouldEqual expectedFullFileList
      refresher.nbDiscoveredFiles shouldBe 3
    }

    it("should exclude previously processed files") {
      val config    = createConfig()
      val refresher = new WorkloadRefresher(config)

      val workload = refresher.refreshWorkload(Set("file1.log"), maxFiles = 10)

      val expectedFilesToProcess = Seq("file2.log", "file3.log")
      val expectedFullFileList   = Set("file2.log", "file3.log", "file1.log")

      workload.filesToProcess shouldEqual expectedFilesToProcess
      workload.fullFileList shouldEqual expectedFullFileList
      refresher.nbDiscoveredFiles shouldBe 3
    }
  }

  describe("getAllCurrentFiles") {

    it("should return all file names from the remote server") {
      val config    = createConfig()
      val refresher = new WorkloadRefresher(config)

      val files = refresher.getAllCurrentFiles

      val expected = Set("file1.log", "file2.log", "file3.log")

      files shouldBe expected
    }
  }

  describe("discoverAllFiles") {

    it("should return all file names without limits") {
      val config    = createConfig()
      val refresher = new WorkloadRefresher(config)

      val files = refresher.discoverAllFiles()

      val expected = Set("file1.log", "file2.log", "file3.log")

      files shouldBe expected
    }
  }

}
