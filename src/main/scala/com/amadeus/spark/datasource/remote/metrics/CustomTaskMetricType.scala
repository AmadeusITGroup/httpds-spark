package com.amadeus.spark.datasource.remote.metrics

/**
 * Enumeration representing different types of bytes throughput metrics.
 */
object CustomTaskMetricType extends Enumeration {

  /** Type alias for CustomTaskMetricType values. */
  type CustomTaskMetricType = Value

  /** Average bytes throughput metric type. */
  val Avg: Value = Value(0, "avg")

  /** Maximum bytes throughput metric type. */
  val Max: Value = Value(1, "max")

  /** Minimum bytes throughput metric type. */
  val Min: Value = Value(2, "min")

}
