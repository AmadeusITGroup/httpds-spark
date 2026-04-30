package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for AvgFilesDownloadedMetric.
 */
class AvgFilesDownloadedMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new AvgFilesDownloadedMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should calculate average files downloaded") {
      val metric      = new AvgFilesDownloadedMetric()
      val taskMetrics = Array(10L, 20L, 30L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "20"
    }

    it("should handle single task metric") {
      val metric      = new AvgFilesDownloadedMetric()
      val taskMetrics = Array(15L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "15"
    }

    it("should handle integer division") {
      val metric      = new AvgFilesDownloadedMetric()
      val taskMetrics = Array(10L, 15L, 28L) // avg = 15
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "17"
    }

    it("should format with comma separator for large values") {
      val metric      = new AvgFilesDownloadedMetric()
      val taskMetrics = Array(1000L, 2000L, 3000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2,000"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new AvgFilesDownloadedMetric()
      metric.name() shouldBe "avg files downloaded"
    }

    it("should have correct description") {
      val metric = new AvgFilesDownloadedMetric()
      metric.description() shouldBe "Average number of files downloaded by any single partition"
    }
  }
}
