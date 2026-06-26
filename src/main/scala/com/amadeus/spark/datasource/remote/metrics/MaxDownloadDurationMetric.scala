package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the maximum download duration across all partitions.
 *
 * This metric helps identify slow partitions and download bottlenecks.
 */
class MaxDownloadDurationMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "max download duration"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Maximum download duration for any partition"

  /**
   * Aggregates metric values from all tasks.
   *
   * For download duration, we take the maximum value to identify the slowest partition.
   *
   * @param taskMetrics array of metric values (in milliseconds) from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      NA
    } else {
      val maxDurationMs = taskMetrics.max
      formatDuration(maxDurationMs)
    }
  }
}
