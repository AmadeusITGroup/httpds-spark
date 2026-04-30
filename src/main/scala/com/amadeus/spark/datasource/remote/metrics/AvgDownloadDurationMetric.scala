package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that aggregates the average download duration across all tasks/partitions.
 */
class AvgDownloadDurationMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "avg download duration"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Average download duration for any partition"

  /**
   * Aggregates metric values from all tasks.
   *
   * For download duration, we take the average value across all partitions.
   *
   * @param taskMetrics array of metric values (in milliseconds) from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      return NA
    }

    val avgDurationMs = taskMetrics.sum / taskMetrics.length
    formatDuration(avgDurationMs)
  }

}
