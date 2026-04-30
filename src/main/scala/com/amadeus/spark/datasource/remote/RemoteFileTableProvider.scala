package com.amadeus.spark.datasource.remote

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * TableProvider implementation for remote file data source.
 */
class RemoteFileTableProvider extends TableProvider with DataSourceRegister with Logging {

  /**
   * Short name used in .format("rest-file").
   *
   * @return the short name for this data source
   */
  override def shortName(): String = RemoteFileTableProvider.SHORT_NAME

  /**
   * Infers the schema for the remote data source.
   *
   * @param options configuration options from the user
   * @return the inferred schema
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = RemoteFileFormat.SCHEMA

  /**
   * Indicates whether this provider supports user-provided schemas.
   *
   * When true, users can provide their own schema via .schema(userSchema).
   *
   * @return true if external metadata (user-provided schema) is supported
   */
  override def supportsExternalMetadata(): Boolean = false

  /**
   * Returns a [[Table]] instance for reading/writing data.
   *
   * @param schema       the schema to use (either inferred or user-provided)
   * @param partitioning any partitioning transforms
   * @param properties   configuration options from the user
   * @return a Table instance
   */
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    new RemoteFileTable(schema, properties)
  }
}

/**
 * Companion object containing constants and utilities for RemoteFileTableProvider.
 */
private object RemoteFileTableProvider {

  /** Short name for the data source, used in .format("rest-file") */
  private val SHORT_NAME: String = "rest-file"
}
