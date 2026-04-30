package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for AvgBytesThroughputMetric.
 */
class AvgBytesThroughputMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new AvgBytesThroughputMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should correctly compute average bytes throughput") {
      val metric      = new AvgBytesThroughputMetric()
      val taskMetrics = Array(1048576L, 2097152L, 3145728L) // 1 MB/s, 2 MB/s, 3 MB/s
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2.00 MB/s"
    }

    it("should handle single task metric") {
      val metric      = new AvgBytesThroughputMetric()
      val taskMetrics = Array(5242880L) // 5 MB/s
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "5.00 MB/s"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new AvgBytesThroughputMetric()
      metric.name() shouldBe "avg bytes throughput"
    }

    it("should have correct description") {
      val metric = new AvgBytesThroughputMetric()
      metric.description() shouldBe "Average MB/s across all partitions"
    }
  }
}
