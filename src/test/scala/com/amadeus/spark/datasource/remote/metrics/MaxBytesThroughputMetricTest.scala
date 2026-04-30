package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MaxBytesThroughputMetric.
 */
class MaxBytesThroughputMetricTest extends AnyFunSpec with Matchers {

  describe("aggregateTaskMetrics") {

    it("should return NA for empty task metrics") {
      val metric = new MaxBytesThroughputMetric()
      metric.aggregateTaskMetrics(Array.empty) shouldBe "N/A"
    }

    it("should return the maximum bytes throughput") {
      val metric      = new MaxBytesThroughputMetric()
      val taskMetrics = Array(1048576L, 3145728L, 2097152L) // 1 MB/s, 3 MB/s, 2 MB/s
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "3.00 MB/s"
    }

    it("should handle single task metric") {
      val metric      = new MaxBytesThroughputMetric()
      val taskMetrics = Array(5242880L) // 5 MB/s
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "5.00 MB/s"
    }

    it("should handle all same values") {
      val metric      = new MaxBytesThroughputMetric()
      val taskMetrics = Array(2097152L, 2097152L, 2097152L) // 2 MB/s
      metric.aggregateTaskMetrics(taskMetrics) shouldBe "2.00 MB/s"
    }
  }

  describe("name and description") {

    it("should have correct name") {
      val metric = new MaxBytesThroughputMetric()
      metric.name() shouldBe "max bytes throughput"
    }

    it("should have correct description") {
      val metric = new MaxBytesThroughputMetric()
      metric.description() shouldBe "Maximum MB/s over all partitions"
    }
  }
}

