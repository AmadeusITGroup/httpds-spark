package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for TotalBytesDownloadedMetric.
 */
class TotalBytesDownloadedMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new TotalBytesDownloadedMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should sum all bytes downloaded") {
      val metric      = new TotalBytesDownloadedMetric()
      val taskMetrics = Array(1048576L, 2097152L, 3145728L) // 1 MB, 2 MB, 3 MB
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "6.00 MB"
    }

    it("should handle single task metric") {
      val metric      = new TotalBytesDownloadedMetric()
      val taskMetrics = Array(5242880L) // 5 MB
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "5.00 MB"
    }

    it("should handle small byte values") {
      val metric      = new TotalBytesDownloadedMetric()
      val taskMetrics = Array(1024L, 2048L, 512L) // small values = 3584 bytes = 3.5 KB
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "3.50 KB"
    }

    it("should format large values correctly") {
      val metric      = new TotalBytesDownloadedMetric()
      val taskMetrics = Array(1073741824L, 1073741824L) // 1 GB + 1 GB
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2.00 GB"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new TotalBytesDownloadedMetric()
      metric.name() shouldBe "total bytes downloaded"
    }

    it("should have correct description") {
      val metric = new TotalBytesDownloadedMetric()
      metric.description() shouldBe "Total bytes downloaded across all partitions"
    }
  }
}
