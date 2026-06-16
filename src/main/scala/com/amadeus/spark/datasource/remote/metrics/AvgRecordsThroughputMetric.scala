package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the average records throughput across all partitions.
 *
 * This metric helps monitor read performance in records per second.
 */
class AvgRecordsThroughputMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "avg records throughput"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Average records per second across all partitions"

  /**
   * Aggregates metric values from all tasks.
   *
   * For throughput, we average the values across all partitions.
   *
   * @param taskMetrics array of throughput values (records/sec) from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      NA
    } else {
      val avgThroughput = taskMetrics.sum / taskMetrics.length
      formatRecordThroughput(avgThroughput)
    }
  }
}
