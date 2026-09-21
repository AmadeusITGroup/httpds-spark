package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MaxDownloadDurationMetric.
 */
class MaxDownloadDurationMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new MaxDownloadDurationMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should return the maximum download duration") {
      val metric      = new MaxDownloadDurationMetric()
      val taskMetrics = Array(1000L, 5000L, 3000L) // in milliseconds
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "5 seconds"
    }

    it("should handle single task metric") {
      val metric      = new MaxDownloadDurationMetric()
      val taskMetrics = Array(2500L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2500 milliseconds"
    }

    it("should handle all same values") {
      val metric      = new MaxDownloadDurationMetric()
      val taskMetrics = Array(1000L, 1000L, 1000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "1 second"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new MaxDownloadDurationMetric()
      metric.name() shouldBe "max download duration"
    }

    it("should have correct description") {
      val metric = new MaxDownloadDurationMetric()
      metric.description() shouldBe "Maximum download duration for any partition"
    }
  }
}
