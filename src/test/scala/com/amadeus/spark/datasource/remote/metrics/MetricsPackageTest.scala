package com.amadeus.spark.datasource.remote.metrics

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for metrics package object formatting functions.
 */
class MetricsPackageTest extends AnyFunSpec with Matchers {

  describe("formatDuration") {

    it("should format milliseconds correctly") {
      formatDuration(500) shouldBe "500 milliseconds"
    }

    it("should format seconds correctly") {
      formatDuration(1000) shouldBe "1 second"
      formatDuration(2000) shouldBe "2 seconds"
      formatDuration(5000) shouldBe "5 seconds"
    }

    it("should format minutes correctly") {
      formatDuration(60000) shouldBe "1 minute"
      formatDuration(120000) shouldBe "2 minutes"
      formatDuration(180000) shouldBe "3 minutes"
    }

    it("should format hours correctly") {
      formatDuration(3600000) shouldBe "1 hour"
      formatDuration(7200000) shouldBe "2 hours"
    }

    it("should format mixed durations correctly") {
      formatDuration(3661000) shouldBe "3661 seconds"
      formatDuration(90500) shouldBe "90500 milliseconds"
    }

    it("should handle zero duration") {
      formatDuration(0) shouldBe "0 millisecond"
    }

    it("should handle negative duration") {
      formatDuration(-1000) shouldBe "-1 seconds"
    }
  }

  describe("formatRecordThroughput") {

    it("should format record throughput with comma separators") {
      formatRecordThroughput(1000) shouldBe "1,000 records/sec"
      formatRecordThroughput(1000000) shouldBe "1,000,000 records/sec"
    }

    it("should format small throughput values") {
      formatRecordThroughput(0) shouldBe "0 records/sec"
      formatRecordThroughput(1) shouldBe "1 records/sec"
      formatRecordThroughput(10) shouldBe "10 records/sec"
      formatRecordThroughput(100) shouldBe "100 records/sec"
    }

    it("should format large throughput values") {
      formatRecordThroughput(123456789) shouldBe "123,456,789 records/sec"
    }
  }

  describe("formatBytesThroughput") {

    it("should format bytes throughput in MB/s") {
      formatBytesThroughput(1048576) shouldBe "1.00 MB/s" // 1 MB
      formatBytesThroughput(2097152) shouldBe "2.00 MB/s" // 2 MB
    }

    it("should format fractional MB/s correctly") {
      formatBytesThroughput(524288) shouldBe "0.50 MB/s"  // 0.5 MB
      formatBytesThroughput(1572864) shouldBe "1.50 MB/s" // 1.5 MB
    }

    it("should format small byte values") {
      formatBytesThroughput(0) shouldBe "0.00 MB/s"
      formatBytesThroughput(1024) shouldBe "0.00 MB/s"  // < 1 KB
      formatBytesThroughput(10240) shouldBe "0.01 MB/s" // 10 KB
    }

    it("should format large byte values") {
      formatBytesThroughput(1073741824) shouldBe "1024.00 MB/s"    // 1 GB
      formatBytesThroughput(10737418240L) shouldBe "10240.00 MB/s" // 10 GB
    }

    it("should round to two decimal places") {
      formatBytesThroughput(1234567) shouldBe "1.18 MB/s"
      formatBytesThroughput(9876543) shouldBe "9.42 MB/s"
    }
  }
}
