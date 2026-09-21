package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the minimum records throughput across all partitions.
 *
 * This metric helps monitor the lowest read performance in records per second.
 */
class MinRecordsThroughputMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "min records throughput"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Minimum records per second across all partitions"

  /**
   * Aggregates metric values from all tasks.
   *
   * For throughput, we take the minimum value across all partitions.
   *
   * @param taskMetrics array of throughput values (records/sec) from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      NA
    } else {
      val minThroughput = taskMetrics.min
      formatRecordThroughput(minThroughput)
    }
  }
}
