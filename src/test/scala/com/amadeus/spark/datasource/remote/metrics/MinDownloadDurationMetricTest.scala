package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MinDownloadDurationMetric.
 */
class MinDownloadDurationMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new MinDownloadDurationMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should return the minimum download duration") {
      val metric      = new MinDownloadDurationMetric()
      val taskMetrics = Array(5000L, 1000L, 3000L) // in milliseconds
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "1 second"
    }

    it("should handle single task metric") {
      val metric      = new MinDownloadDurationMetric()
      val taskMetrics = Array(2500L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2500 milliseconds"
    }

    it("should handle all same values") {
      val metric      = new MinDownloadDurationMetric()
      val taskMetrics = Array(1000L, 1000L, 1000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "1 second"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new MinDownloadDurationMetric()
      metric.name() shouldBe "min download duration"
    }

    it("should have correct description") {
      val metric = new MinDownloadDurationMetric()
      metric.description() shouldBe "Minimum download duration for any partition"
    }
  }
}
