package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MinRecordsThroughputMetric.
 */
class MinRecordsThroughputMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new MinRecordsThroughputMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should return the minimum records throughput") {
      val metric      = new MinRecordsThroughputMetric()
      val taskMetrics = Array(3000L, 1000L, 2000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "1,000 records/sec"
    }

    it("should handle single task metric") {
      val metric      = new MinRecordsThroughputMetric()
      val taskMetrics = Array(5000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "5,000 records/sec"
    }

    it("should handle all same values") {
      val metric      = new MinRecordsThroughputMetric()
      val taskMetrics = Array(2000L, 2000L, 2000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2,000 records/sec"
    }

    it("should handle small throughput values") {
      val metric      = new MinRecordsThroughputMetric()
      val taskMetrics = Array(100L, 50L, 75L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "50 records/sec"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new MinRecordsThroughputMetric()
      metric.name() shouldBe "min records throughput"
    }

    it("should have correct description") {
      val metric = new MinRecordsThroughputMetric()
      metric.description() shouldBe "Minimum records per second across all partitions"
    }
  }
}
