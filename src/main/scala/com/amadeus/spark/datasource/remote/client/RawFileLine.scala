package com.amadeus.spark.datasource.remote.client

import com.amadeus.spark.datasource.remote.RemoteFileFormat
import org.apache.spark.sql.catalyst.InternalRow

import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Represents a complete log file with metadata and content.
 *
 * Design: Each instance represents ONE complete log file (not a line).
 * The binary content is split by b"|==|\n" to separate:
 * - logMetadata: UTF-8 header (key:value pairs as JSON string)
 * - logContent: encrypted binary data after the separator
 * - rawFileBinary: complete file if split fails
 *
 * @param sourceFileMetadata metadata from HTTP response
 * @param logMetadata        parsed log metadata as JSON string (from header before |==|)
 * @param logContent         encrypted log content binary (after |==|)
 * @param rawFileBinary      complete raw file binary (fallback if split fails)
 * @param error              error message if processing failed
 */
case class RawFileLine(
    sourceFileMetadata: RemoteFile,
    logMetadata: Option[String] = None,
    logContent: Option[Array[Byte]] = None,
    rawFileBinary: Option[Array[Byte]] = None,
    downloadTimestamp: Timestamp,
    error: Option[String] = None
) {

  /**
   * Converts this RawFileLine to a Spark SQL InternalRow based on the provided schema.
   *
   * The schema can partially or fully match RemoteFileFormat.SCHEMA. The function will
   * dynamically map available fields based on the schema structure:
   * - sourceFile (StructType): contains name, fetchedAt, and HTTP metadata
   * - logMetadata (StringType): JSON string with parsed log metadata
   * - logContent (BinaryType): encrypted log content
   * - rawFileBinary (BinaryType): complete file if split failed
   * - downloadTimestamp (TimestampType): when the file was downloaded
   * - error (StringType): error message
   *
   * If a field is not present in the schema, it will be skipped.
   *
   * @param schema the schema to use for conversion (can be partial or full RemoteFileFormat.SCHEMA)
   * @return an InternalRow representing this RawFileLine
   */
  def toInternalRow(schema: StructType): InternalRow = {
    val values = new Array[Any](schema.fields.length)

    schema.fields.zipWithIndex.foreach {
      case (field, idx) =>
        val value: Any = field.name match {
          case RemoteFileFormat.SOURCE_FILE =>
            // Handle sourceFile struct field
            field.dataType match {
              case structType: StructType => convertSourceFileToStruct(structType)
              case _ => null // Unexpected type, return null
            }
          case RemoteFileFormat.LOG_METADATA => logMetadata.map(UTF8String.fromString).orNull
          case RemoteFileFormat.LOG_CONTENT => logContent.orNull
          case RemoteFileFormat.RAW_FILE_BINARY => rawFileBinary.orNull
          case RemoteFileFormat.DOWNLOAD_TIMESTAMP => downloadTimestamp.getTime * 1000L // Convert to microseconds
          case RemoteFileFormat.ERROR => error.map(UTF8String.fromString).orNull
          case _ => null
        }
        values(idx) = value
    }

    new GenericInternalRow(values)
  }

  /**
   * Converts the sourceFileMetadata to a struct based on the provided schema.
   * Handles partial schemas that may include HTTP response metadata fields.
   *
   * @param structType the struct schema for sourceFile
   * @return a GenericInternalRow representing the sourceFile struct
   */
  private def convertSourceFileToStruct(structType: StructType): InternalRow = {
    val structValues = new Array[Any](structType.fields.length)

    structType.fields.zipWithIndex.foreach { case (field, idx) =>
      val value: Any = field.name match {
        case RemoteFileFormat.SOURCE_FILE_NAME          => UTF8String.fromString(sourceFileMetadata.name)
        case RemoteFileFormat.SOURCE_FILE_FETCHED_AT    => sourceFileMetadata.fetchedAt.getTime * 1000L // Convert to microseconds
        case RemoteFileFormat.SOURCE_FILE_SIZE          => sourceFileMetadata.fileSize.map(Long.box).orNull
        case RemoteFileFormat.SOURCE_FILE_LAST_MODIFIED => sourceFileMetadata.lastModified.map(_.getTime * 1000L).map(Long.box).orNull
        case RemoteFileFormat.SOURCE_FILE_ETAG          => sourceFileMetadata.etag.map(UTF8String.fromString).orNull
        case RemoteFileFormat.SOURCE_FILE_REQUEST_ID    => sourceFileMetadata.requestId.map(UTF8String.fromString).orNull
        case _                                          => null
      }
      structValues(idx) = value
    }

    new GenericInternalRow(structValues)
  }
}
