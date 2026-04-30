package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for AvgRecordsThroughputMetric.
 */
class AvgRecordsThroughputMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new AvgRecordsThroughputMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should correctly compute average records throughput") {
      val metric      = new AvgRecordsThroughputMetric()
      val taskMetrics = Array(1000L, 2000L, 3000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2,000 records/sec"
    }

    it("should handle single task metric") {
      val metric      = new AvgRecordsThroughputMetric()
      val taskMetrics = Array(5000L)
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "5,000 records/sec"
    }

    it("should handle integer division") {
      val metric      = new AvgRecordsThroughputMetric()
      val taskMetrics = Array(1000L, 1500L, 2800L) // avg = 1500
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "1,766 records/sec"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new AvgRecordsThroughputMetric()
      metric.name() shouldBe "avg records throughput"
    }

    it("should have correct description") {
      val metric = new AvgRecordsThroughputMetric()
      metric.description() shouldBe "Average records per second across all partitions"
    }
  }
}

