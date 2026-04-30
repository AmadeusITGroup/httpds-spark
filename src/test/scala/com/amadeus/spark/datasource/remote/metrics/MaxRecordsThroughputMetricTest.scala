package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MaxRecordsThroughputMetric.
 */
class MaxRecordsThroughputMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new MaxRecordsThroughputMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should return the maximum records throughput") {
      val metric      = new MaxRecordsThroughputMetric()
      val taskMetrics = Array(1000L, 3000L, 2000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "3,000 records/sec"
    }

    it("should handle single task metric") {
      val metric      = new MaxRecordsThroughputMetric()
      val taskMetrics = Array(5000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "5,000 records/sec"
    }

    it("should handle all same values") {
      val metric      = new MaxRecordsThroughputMetric()
      val taskMetrics = Array(2000L, 2000L, 2000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2,000 records/sec"
    }

    it("should handle large throughput values") {
      val metric      = new MaxRecordsThroughputMetric()
      val taskMetrics = Array(1000000L, 500000L, 750000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "1,000,000 records/sec"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new MaxRecordsThroughputMetric()
      metric.name() shouldBe "max records throughput"
    }

    it("should have correct description") {
      val metric = new MaxRecordsThroughputMetric()
      metric.description() shouldBe "Maximum records per second across all partitions"
    }
  }
}

