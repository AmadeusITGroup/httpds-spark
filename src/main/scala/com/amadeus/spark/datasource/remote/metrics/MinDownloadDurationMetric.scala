package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the minimum download duration across all tasks/partitions.
 */
class MinDownloadDurationMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "min download duration"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Minimum download duration for any partition"

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
      val minDurationMs = taskMetrics.min
      formatDuration(minDurationMs)
    }
  }

}
