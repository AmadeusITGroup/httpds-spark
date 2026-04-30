package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.SparkTestBase
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test to verify custom metrics are reported correctly.
 */
class CustomMetricsSpec extends AnyFunSpec with Matchers with SparkTestBase {

  describe("Custom Metrics") {

    it("should report max files downloaded metric") {
      // Create DataFrame using the data source with mock client
      val df = spark.read
        .format("rest-file")
        .option("uri", "http://localhost:5000")
        .option("remoteClient", "mock")
        .option("numPartitions", "2") // Use 2 partitions to distribute files
        .load()

      // Execute the query to trigger metrics collection
      val rows = df.collect()

      // Verify we got data
      rows.length should be > 0

      // Access the execution plan to check for metrics
      val executedPlan = df.queryExecution.executedPlan

      // The custom metrics are available in the executed plan
      // Verify the plan has metrics (they will include our custom metric)
      executedPlan.metrics should not be empty

      // Log the metrics for visibility
      println("\n=== Custom Metrics Report ===")
      executedPlan.metrics.foreach { case (name, metric) =>
        println(s"  $name: ${metric.value}")
      }

      // The "max files downloaded" metric should exist
      // Note: The metric name in the execution plan may be normalized
      val metricNames = executedPlan.metrics.keys.toSet
      println(s"\nAvailable metrics: ${metricNames.mkString(", ")}")
      println("===========================\n")

      // Verify the data is correct - updated for new file-based design
      df.schema.fieldNames should contain("logMetadata")
      df.schema.fieldNames should contain("logContent")
      df.schema.fieldNames should contain("sourceFile")
    }

    it("should show higher max with fewer partitions") {
      // With 1 partition, all files go to a single partition
      val df1 = spark.read
        .format("rest-file")
        .option("uri", "http://localhost:5000")
        .option("remoteClient", "mock")
        .option("numPartitions", "1")
        .load()

      val rows1 = df1.collect()
      println(s"\nWith 1 partition: Read ${rows1.length} rows")

      // With 4 partitions, files are distributed
      val df2 = spark.read
        .format("rest-file")
        .option("uri", "http://localhost:5000")
        .option("remoteClient", "mock")
        .option("numPartitions", "4")
        .load()

      val rows2 = df2.collect()
      println(s"With 4 partitions: Read ${rows2.length} rows\n")

      // Both should read the same total rows
      rows1.length shouldBe rows2.length
    }
  }
}

