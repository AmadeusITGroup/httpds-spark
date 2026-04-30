package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.metrics._
import com.amadeus.spark.datasource.remote.read.RemoteFileBatch
import com.amadeus.spark.datasource.remote.streaming.RemoteMicroBatchStream
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.OptionalLong
import scala.collection.JavaConverters._

class RemoteFileScanTest extends AnyFunSpec with Matchers {

  private val schema = StructType(
    Seq(
      StructField("value", StringType, nullable = true)
    )
  )

  private def defaultOptions: CaseInsensitiveStringMap = {
    new CaseInsensitiveStringMap(
      Map(
        "uri"          -> "http://localhost:5000",
        "remoteClient" -> "testClient"
      ).asJava
    )
  }

  private def createScan(opts: CaseInsensitiveStringMap = defaultOptions): RemoteFileScan = {
    new RemoteFileScan(schema, opts)
  }

  describe("readSchema()") {
    it("should return the schema provided at construction") {
      val scan = createScan()
      scan.readSchema() shouldEqual schema
    }
  }

  describe("toBatch") {
    it("should return a RemoteFileBatch instance") {
      val scan = createScan()
      scan.toBatch shouldBe a[RemoteFileBatch]
    }
  }

  describe("toMicroBatchStream()") {
    it("should return a RemoteMicroBatchStream instance") {
      val scan = createScan()
      scan.toMicroBatchStream("/tmp/checkpoint") shouldBe a[RemoteMicroBatchStream]
    }
  }

  describe("supportedCustomMetrics()") {
    it("should return a non-empty array of metrics") {
      val scan    = createScan()
      val metrics = scan.supportedCustomMetrics()

      val expectedMetrics = Array(
        new MaxFilesDownloadedMetric(),
        new AvgFilesDownloadedMetric(),
        new MinFilesDownloadedMetric(),
        new TotalBytesDownloadedMetric(),
        new AvgDownloadDurationMetric(),
        new MaxDownloadDurationMetric(),
        new MinDownloadDurationMetric(),
        new AvgRecordsThroughputMetric(),
        new MaxRecordsThroughputMetric(),
        new MinRecordsThroughputMetric(),
        new AvgBytesThroughputMetric(),
        new MaxBytesThroughputMetric(),
        new MinBytesThroughputMetric()
      )

      metrics.length shouldBe expectedMetrics.length
      metrics zip expectedMetrics foreach { case (actual, expected) =>
        actual.getClass shouldEqual expected.getClass
      }
    }
  }

  describe("estimateStatistics()") {
    it("should return 100 MB as sizeInBytes") {
      val scan  = createScan()
      val stats = scan.estimateStatistics()

      stats.sizeInBytes() shouldEqual OptionalLong.of(100 * 1024 * 1024)
    }

    it("should return empty for numRows") {
      val scan  = createScan()
      val stats = scan.estimateStatistics()

      stats.numRows() shouldEqual OptionalLong.empty()
    }
  }
}
