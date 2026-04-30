package com.amadeus.spark.datasource.remote

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * ScanBuilder for HTTP streaming.
 *
 * This builder creates scans for streaming reads from HTTP endpoint.
 *
 * @param schema  the full table schema
 * @param options configuration options
 */
class RemoteFileScanBuilder(schema: StructType, options: CaseInsensitiveStringMap) extends ScanBuilder {

  /** The schema after any column pruning (defaults to full schema) */
  private val requiredSchema: StructType = schema

  /**
   * Builds the scan.
   */
  override def build(): Scan = {
    new RemoteFileScan(requiredSchema, options)
  }
}
