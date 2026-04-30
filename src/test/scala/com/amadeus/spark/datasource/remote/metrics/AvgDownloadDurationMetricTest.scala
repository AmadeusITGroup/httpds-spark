package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for AvgDownloadDurationMetric.
 */
class AvgDownloadDurationMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new AvgDownloadDurationMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should calculate average download duration") {
      val metric      = new AvgDownloadDurationMetric()
      val taskMetrics = Array(1000L, 2000L, 3000L) // in milliseconds
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2 seconds"
    }

    it("should handle single task metric") {
      val metric      = new AvgDownloadDurationMetric()
      val taskMetrics = Array(2500L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2500 milliseconds"
    }

    it("should handle integer division") {
      val metric      = new AvgDownloadDurationMetric()
      val taskMetrics = Array(1000L, 1500L, 2100L) // avg = 1500
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "1533 milliseconds"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new AvgDownloadDurationMetric()
      metric.name() shouldBe "avg download duration"
    }

    it("should have correct description") {
      val metric = new AvgDownloadDurationMetric()
      metric.description() shouldBe "Average download duration for any partition"
    }
  }
}
