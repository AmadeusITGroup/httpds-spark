package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import com.amadeus.spark.datasource.remote.helpers.MetricsHelper
import com.amadeus.spark.datasource.remote.read.RemoteFileBatch
import com.amadeus.spark.datasource.remote.streaming.RemoteMicroBatchStream
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.OptionalLong

/**
 * Scan implementation for remote streaming and batch reads.
 *
 * This Scan provides both micro-batch stream and batch implementations
 * for reading data from remote APIs.
 *
 * @param schema  the schema to read
 * @param options configuration options
 */
class RemoteFileScan(schema: StructType, options: CaseInsensitiveStringMap) extends Scan with SupportsReportStatistics with Logging {

  /** Parsed configuration options */
  private val config: RemoteFileDataSourceOptions = RemoteFileDataSourceOptions.fromMap(options)

  /**
   * Returns the schema of data to be read.
   */
  override def readSchema(): StructType = schema

  /**
   * Creates a Batch for batch processing.
   *
   * This enables the same scan to work for both batch reads (spark.read)
   * and streaming reads (spark.readStream).
   *
   * @return a Batch instance
   */
  override def toBatch: Batch = {
    logDebug(s"Creating batch for scan: ${description()}")
    new RemoteFileBatch(schema, options)
  }

  /**
   * Returns a description of this scan for debugging.
   */
  override def description(): String = {
    s"RestScan[schema=${schema.simpleString},  pollingInterval=${config.pollingInterval.toMillis}ms,  maxFilesPerTrigger=${config.maxFilesPerTrigger}]"
  }

  /**
   * Creates a MicroBatchStream for structured streaming.
   *
   * Micro-batch provides:
   * - Higher latency but more features
   * - Support for aggregations and joins
   * - Exactly-once semantics with checkpointing
   *
   * @param checkpointLocation the checkpoint location
   * @return a micro-batch stream
   */
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    logDebug(s"Creating micro-batch stream with checkpoint: $checkpointLocation")
    new RemoteMicroBatchStream(schema, options)
  }

  /**
   * Declares the custom metrics supported by this batch.
   *
   * This method is called by Spark to register custom metrics for display in the UI.
   * Each CustomMetric defines how task-level metrics are aggregated.
   *
   * @return array of custom metrics
   */
  override def supportedCustomMetrics(): Array[CustomMetric] = MetricsHelper.supportedMetrics()

  /**
   * Estimates statistics for this scan, such as size in bytes and number of rows.
   *
   * This information can be used by Spark's optimizer for planning joins and other operations.
   * For now, we return a moderate size estimate to avoid broadcasting small datasets.
   *
   * @return estimated statistics
   */
  override def estimateStatistics(): Statistics = {
    new Statistics {

      /**
       * Estimated size in bytes.
       *
       * This affects broadcast join thresholds.
       * Return empty if unknown.
       */
      override def sizeInBytes(): OptionalLong = {
        // For now, return a moderate estimate to avoid broadcasting
        OptionalLong.of(100 * 1024 * 1024) // 100 MB
      }

      /**
       * Estimated number of rows.
       *
       * Return empty if unknown.
       */
      override def numRows(): OptionalLong = {
        OptionalLong.empty()
      }
    }
  }
}
