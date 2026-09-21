package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

class MinBytesThroughputMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "min bytes throughput"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Minimum MB/s over all partitions"

  /**
   * Aggregates metric values from all tasks.
   *
   * For throughput, we take the minimum value over all partitions.
   * The task metrics are in bytes per second, we convert to MB/s.
   *
   * @param taskMetrics array of throughput values (bytes/sec) from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      NA
    } else {
      val minBytesPerSec = taskMetrics.min
      formatBytesThroughput(minBytesPerSec)
    }
  }
}
