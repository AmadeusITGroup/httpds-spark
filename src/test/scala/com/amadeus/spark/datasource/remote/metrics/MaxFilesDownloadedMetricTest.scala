package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MaxFilesDownloadedMetric.
 */
class MaxFilesDownloadedMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new MaxFilesDownloadedMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should return the maximum files downloaded") {
      val metric      = new MaxFilesDownloadedMetric()
      val taskMetrics = Array(5L, 10L, 3L, 8L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "10"
    }

    it("should handle single task metric") {
      val metric      = new MaxFilesDownloadedMetric()
      val taskMetrics = Array(7L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "7"
    }

    it("should handle all same values") {
      val metric      = new MaxFilesDownloadedMetric()
      val taskMetrics = Array(5L, 5L, 5L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "5"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new MaxFilesDownloadedMetric()
      metric.name() shouldBe "max files downloaded"
    }

    it("should have correct description") {
      val metric = new MaxFilesDownloadedMetric()
      metric.description() shouldBe "Maximum number of files downloaded by any single partition"
    }
  }
}
