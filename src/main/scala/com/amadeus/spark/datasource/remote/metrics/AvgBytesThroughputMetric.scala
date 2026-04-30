package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the average bytes throughput across all partitions.
 *
 * This metric helps monitor read performance in MB/s.
 */
class AvgBytesThroughputMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "avg bytes throughput"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Average MB/s across all partitions"

  /**
   * Aggregates metric values from all tasks.
   *
   * For throughput, we average the values across all partitions.
   * The task metrics are in bytes per second, we convert to MB/s.
   *
   * @param taskMetrics array of throughput values (bytes/sec) from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      return NA
    }

    val avgBytesPerSec = taskMetrics.sum / taskMetrics.length
    formatBytesThroughput(avgBytesPerSec)
  }
}
