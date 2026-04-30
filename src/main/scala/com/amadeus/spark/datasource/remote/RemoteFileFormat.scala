package com.amadeus.spark.datasource.remote

import org.apache.spark.sql.types._

/**
 * remote file format schema definition.
 *
 * Schema design: Each row represents a complete log file with:
 * - HTTP response metadata (sourceFile struct)
 * - Parsed log metadata as JSON string (logMetadata)
 * - Encrypted log content as binary (logContent)
 * - Optional raw file binary if parsing fails (rawFileBinary)
 * - Error messages if any (error)
 */
object RemoteFileFormat {

  /** Source file column name */
  val SOURCE_FILE = "sourceFile"

  /** Source file name column name */
  val SOURCE_FILE_NAME = "name"

  /** Fetch timestamp column name */
  val SOURCE_FILE_FETCHED_AT = "fetchedAt"

  /** HTTP response metadata field names */
  val SOURCE_FILE_SIZE          = "fileSize"
  val SOURCE_FILE_LAST_MODIFIED = "lastModified"
  val SOURCE_FILE_ETAG          = "etag"
  val SOURCE_FILE_REQUEST_ID    = "requestId"

  /** Log metadata JSON column (parsed from header before |==|) */
  val LOG_METADATA = "logMetadata"

  /** Encrypted log content (binary data after |==|) */
  val LOG_CONTENT = "logContent"

  /** Raw file binary (fallback when split by |==| fails) */
  val RAW_FILE_BINARY = "rawFileBinary"

  /** Download timestamp column name (top-level for convenience) */
  val DOWNLOAD_TIMESTAMP = "downloadTimestamp"

  /** Error column name */
  val ERROR = "error"

  /** Default schema for remote file format */
  val SCHEMA: StructType = StructType(
    Seq(
      StructField(
        SOURCE_FILE,
        StructType(
          Seq(
            StructField(SOURCE_FILE_NAME, StringType, nullable = false, new MetadataBuilder().putString("description", "Source file name").build()),
            StructField(SOURCE_FILE_FETCHED_AT, TimestampType, nullable = false, new MetadataBuilder().putString("description", "Source file fetch timestamp").build()),
            StructField(SOURCE_FILE_SIZE, LongType, nullable = true, new MetadataBuilder().putString("description", "File size in bytes from Content-Length header").build()),
            StructField(
              SOURCE_FILE_LAST_MODIFIED,
              TimestampType,
              nullable = true,
              new MetadataBuilder().putString("description", "Last-Modified timestamp from HTTP header").build()
            ),
            StructField(SOURCE_FILE_ETAG, StringType, nullable = true, new MetadataBuilder().putString("description", "ETag from HTTP response").build()),
            StructField(
              SOURCE_FILE_REQUEST_ID,
              StringType,
              nullable = true,
              new MetadataBuilder().putString("description", "Request ID from HTTP response (x-amz-request-id)").build()
            )
          )
        ),
        nullable = false,
        new MetadataBuilder().putString("description", "Source file metadata from HTTP response").build()
      ),
      StructField(
        LOG_METADATA,
        StringType,
        nullable = true,
        new MetadataBuilder().putString("description", "Log metadata from file content if any.").build()
      ),
      StructField(
        LOG_CONTENT,
        BinaryType,
        nullable = true,
        new MetadataBuilder().putString("description", "File log content ready for processing").build()
      ),
      StructField(
        RAW_FILE_BINARY,
        BinaryType,
        nullable = true,
        new MetadataBuilder().putString("description", "Complete raw file binary").build()
      ),
      StructField(
        DOWNLOAD_TIMESTAMP,
        TimestampType,
        nullable = false,
        new MetadataBuilder().putString("description", "Timestamp when the file was downloaded from the remote source").build()
      ),
      StructField(ERROR, StringType, nullable = true, new MetadataBuilder().putString("description", "Error message if processing failed").build())
    )
  )

}
