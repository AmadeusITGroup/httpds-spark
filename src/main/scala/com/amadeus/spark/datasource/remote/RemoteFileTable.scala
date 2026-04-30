package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import java.util

/**
 * Table implementation for remote file data source.
 *
 * This table supports both batch and streaming read capabilities.
 *
 * @param tableSchema the schema of the table
 * @param properties  the table properties
 */
class RemoteFileTable(tableSchema: StructType, properties: util.Map[String, String]) extends Table with SupportsRead with Logging {

  /** Configuration options for the remote file data source. */
  private val config: RemoteFileDataSourceOptions = RemoteFileDataSourceOptions.fromMap(properties)

  /**
   * Returns the schema of this table.
   *
   * @return the table schema
   */
  override def schema(): StructType = tableSchema

  /**
   * Declares the capabilities supported by this table.
   *
   * This informs Spark about what operations can be performed:
   * - BATCH_READ: Supports reading data in batch mode
   * - MICRO_BATCH_READ: Supports streaming reads in micro-batch mode
   *
   * @return a set of supported capabilities
   */
  override def capabilities(): java.util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ).asJava
  }

  /**
   * Creates a [[ScanBuilder]] for constructing read operations.
   *
   * The ScanBuilder allows Spark to configure the read operation,
   * including filter pushdown and column pruning optimizations.
   *
   * For streaming reads (when configured for Incapsula), this returns
   * a streaming-capable scan builder.
   *
   * @param options configuration options for the scan
   * @return a new ScanBuilder instance
   */
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logDebug(s"Creating scan builder with options: ${options.keySet()}")

    // Merge table properties with scan options (scan options take precedence)
    val mergedOptions = new CaseInsensitiveStringMap(
      (this.properties.asScala ++ options.asScala).asJava
    )

    new RemoteFileScanBuilder(tableSchema, mergedOptions)
  }

  override def name(): String = s"RestFileTable(${config.serverUri})"
}
