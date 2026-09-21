package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the maximum bytes throughput (in MB/s) over all partitions.
 *
 * This metric helps monitor the data transfer performance of the remote data source.
 */
class MaxBytesThroughputMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "max bytes throughput"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Maximum MB/s over all partitions"

  /**
   * Aggregates metric values from all tasks.
   *
   * For throughput, we take the maximum value over all partitions.
   * The task metrics are in bytes per second, we convert to MB/s.
   *
   * @param taskMetrics array of throughput values (bytes/sec) from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      NA
    } else {
      val maxBytesPerSec = taskMetrics.max
      formatBytesThroughput(maxBytesPerSec)
    }
  }
}
