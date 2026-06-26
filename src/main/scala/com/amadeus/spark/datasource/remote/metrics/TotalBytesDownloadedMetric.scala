package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the total bytes downloaded across all partitions.
 */
class TotalBytesDownloadedMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "total bytes downloaded"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Total bytes downloaded across all partitions"

  /**
   * Aggregates metric values from all tasks.
   *
   * For total bytes downloaded, we sum the values across all partitions.
   *
   * @param taskMetrics array of metric values (in bytes) from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      NA
    } else {
      val totalBytes = taskMetrics.sum
      formatBytes(totalBytes)
    }
  }

  /**
   * Formats bytes into a human-readable string (e.g., KB, MB, GB).
   *
   * @param bytes the number of bytes
   * @return formatted string
   */
  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024) {
      s"$bytes B"
    } else if (bytes < 1024 * 1024) {
      f"${bytes / 1024.0}%.2f KB"
    } else if (bytes < 1024 * 1024 * 1024) {
      f"${bytes / (1024.0 * 1024.0)}%.2f MB"
    } else {
      f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.2f GB"
    }
  }
}
