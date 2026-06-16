package com.amadeus.spark.datasource.remote.client

import com.amadeus.spark.datasource.remote.RemoteFileFormat
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

/**
 * Unit tests for RawFileLine.
 * Updated for new design: each RawFileLine represents a complete file (not a line).
 */
// ScalaTest assertions and Java API checks require null comparisons — Java interop test patterns
// scalafix:off DisableSyntax.null
class RawFileLineTest extends AnyFunSpec with Matchers {

  // Test fixture data
  val testTimestamp   = new Timestamp(1609459200000L)             // 2021-01-01 00:00:00
  val testFileName    = "test-file.log"
  val testLogMetadata = """{"accountId":1147454,"configId":3088,"format":"LEEF"}"""
  val testLogContent  = Array[Byte](0x48, 0x65, 0x6c, 0x6c, 0x6f) // "Hello"
  val testRawBinary   = Array[Byte](0x01, 0x02, 0x03)
  val testError       = "Download failed: HTTP 500"

  // Remote file with HTTP metadata
  val remoteFile = RemoteFile(
    name = testFileName,
    fetchedAt = testTimestamp,
    fileSize = Some(12345L),
    lastModified = Some(testTimestamp),
    etag = Some("abc123"),
    requestId = Some("req-123")
  )

  describe("toInternalRow") {

    describe("with full schema") {

      it("should convert RawFileLine with parsed log content to InternalRow") {
        val rawFileLine = RawFileLine(
          sourceFileMetadata = remoteFile,
          logMetadata = Some(testLogMetadata),
          logContent = Some(testLogContent),
          downloadTimestamp = testTimestamp
        )
        val row = rawFileLine.toInternalRow(RemoteFileFormat.SCHEMA)

        row should not be null
        row.numFields shouldBe 6

        // Check sourceFile struct (6 fields: name, fetchedAt, fileSize, lastModified, etag, requestId)
        val sourceFileStruct = row.getStruct(0, 6)
        sourceFileStruct.getString(0) shouldBe testFileName
        sourceFileStruct.getLong(1) shouldBe testTimestamp.getTime * 1000L
        sourceFileStruct.getLong(2) shouldBe 12345L
        sourceFileStruct.getLong(3) shouldBe testTimestamp.getTime * 1000L
        sourceFileStruct.getString(4) shouldBe "abc123"
        sourceFileStruct.getString(5) shouldBe "req-123"

        // Check logMetadata
        row.getString(1) shouldBe testLogMetadata

        // Check logContent
        row.getBinary(2) shouldBe testLogContent

        // Check rawFileBinary (should be null)
        row.isNullAt(3) shouldBe true

        // Check downloadTimestamp
        row.getLong(4) shouldBe testTimestamp.getTime * 1000L

        // Check error (should be null)
        row.isNullAt(5) shouldBe true
      }

      it("should convert RawFileLine with raw binary fallback to InternalRow") {
        val rawFileLine = RawFileLine(
          sourceFileMetadata = remoteFile,
          rawFileBinary = Some(testRawBinary),
          downloadTimestamp = testTimestamp,
          error = Some("Failed to parse: separator not found")
        )
        val row = rawFileLine.toInternalRow(RemoteFileFormat.SCHEMA)

        row should not be null
        row.numFields shouldBe 6

        // Check logMetadata (should be null)
        row.isNullAt(1) shouldBe true

        // Check logContent (should be null)
        row.isNullAt(2) shouldBe true

        // Check rawFileBinary
        row.getBinary(3) shouldBe testRawBinary

        // Check downloadTimestamp
        row.getLong(4) shouldBe testTimestamp.getTime * 1000L

        // Check error
        row.getString(5) shouldBe "Failed to parse: separator not found"
      }

      it("should convert RawFileLine with error only to InternalRow") {
        val rawFileLine = RawFileLine(remoteFile, downloadTimestamp = testTimestamp, error = Some(testError))
        val row         = rawFileLine.toInternalRow(RemoteFileFormat.SCHEMA)

        row should not be null
        row.numFields shouldBe 6

        // Check sourceFile struct
        val sourceFileStruct = row.getStruct(0, 6)
        sourceFileStruct.getString(0) shouldBe testFileName
        sourceFileStruct.getLong(1) shouldBe testTimestamp.getTime * 1000L

        // Check logMetadata (should be null)
        row.isNullAt(1) shouldBe true

        // Check logContent (should be null)
        row.isNullAt(2) shouldBe true

        // Check rawFileBinary (should be null)
        row.isNullAt(3) shouldBe true

        // Check downloadTimestamp
        row.getLong(4) shouldBe testTimestamp.getTime * 1000L

        // Check error
        row.getString(5) shouldBe testError
      }

      it("should convert RawFileLine with minimal metadata to InternalRow") {
        val minimalRemoteFile = RemoteFile(name = testFileName, fetchedAt = testTimestamp)
        val rawFileLine       = RawFileLine(minimalRemoteFile, downloadTimestamp = testTimestamp)
        val row               = rawFileLine.toInternalRow(RemoteFileFormat.SCHEMA)

        row should not be null
        row.numFields shouldBe 6

        // Check sourceFile struct
        val sourceFileStruct = row.getStruct(0, 6)
        sourceFileStruct.getString(0) shouldBe testFileName
        sourceFileStruct.getLong(1) shouldBe testTimestamp.getTime * 1000L
        // Optional fields should be null
        sourceFileStruct.isNullAt(2) shouldBe true // fileSize
        sourceFileStruct.isNullAt(3) shouldBe true // lastModified
        sourceFileStruct.isNullAt(4) shouldBe true // etag
        sourceFileStruct.isNullAt(5) shouldBe true // requestId

        // All content fields should be null
        row.isNullAt(1) shouldBe true // logMetadata
        row.isNullAt(2) shouldBe true // logContent
        row.isNullAt(3) shouldBe true // rawFileBinary

        // Check downloadTimestamp
        row.getLong(4) shouldBe testTimestamp.getTime * 1000L

        // Check error
        row.isNullAt(5) shouldBe true // error
      }
    }

    describe("with downloadTimestamp") {

      it("should correctly convert downloadTimestamp to microseconds") {
        val downloadTime = new Timestamp(1640995200000L) // 2022-01-01 00:00:00
        val rawFileLine = RawFileLine(
          sourceFileMetadata = remoteFile,
          downloadTimestamp = downloadTime
        )
        val row = rawFileLine.toInternalRow(RemoteFileFormat.SCHEMA)

        row should not be null
        // Check downloadTimestamp field (index 4)
        row.getLong(4) shouldBe downloadTime.getTime * 1000L // Converted to microseconds
      }

      it("should handle different download and fetch timestamps") {
        val fetchTime         = new Timestamp(1609459200000L)
        val downloadTime      = new Timestamp(1640995200000L)
        val fileWithFetchTime = RemoteFile(name = "test.log", fetchedAt = fetchTime)
        val rawFileLine = RawFileLine(
          sourceFileMetadata = fileWithFetchTime,
          downloadTimestamp = downloadTime
        )
        val row = rawFileLine.toInternalRow(RemoteFileFormat.SCHEMA)

        // sourceFile.fetchedAt should be fetchTime
        val sourceFileStruct = row.getStruct(0, 6)
        sourceFileStruct.getLong(1) shouldBe fetchTime.getTime * 1000L

        // downloadTimestamp should be downloadTime (different from fetchTime)
        row.getLong(4) shouldBe downloadTime.getTime * 1000L
      }
    }

    describe("with partial schema") {

      it("should handle schema with only sourceFile field") {
        val partialSchema = StructType(
          Seq(
            StructField(
              RemoteFileFormat.SOURCE_FILE,
              StructType(
                Seq(
                  StructField(RemoteFileFormat.SOURCE_FILE_NAME, StringType, nullable = false),
                  StructField(RemoteFileFormat.SOURCE_FILE_FETCHED_AT, TimestampType, nullable = false),
                  StructField(RemoteFileFormat.SOURCE_FILE_SIZE, LongType, nullable = true),
                  StructField(RemoteFileFormat.SOURCE_FILE_LAST_MODIFIED, TimestampType, nullable = true),
                  StructField(RemoteFileFormat.SOURCE_FILE_ETAG, StringType, nullable = true),
                  StructField(RemoteFileFormat.SOURCE_FILE_REQUEST_ID, StringType, nullable = true)
                )
              ),
              nullable = false
            )
          )
        )

        val rawFileLine = RawFileLine(
          sourceFileMetadata = remoteFile,
          logMetadata = Some(testLogMetadata),
          downloadTimestamp = testTimestamp
        )
        val row = rawFileLine.toInternalRow(partialSchema)

        row should not be null
        row.numFields shouldBe 1

        val sourceFileStruct = row.getStruct(0, 6)
        sourceFileStruct.getString(0) shouldBe testFileName
        sourceFileStruct.getLong(1) shouldBe testTimestamp.getTime * 1000L
      }

      it("should handle schema with logMetadata and logContent only") {
        val partialSchema = StructType(
          Seq(
            StructField(RemoteFileFormat.LOG_METADATA, StringType, nullable = true),
            StructField(RemoteFileFormat.LOG_CONTENT, BinaryType, nullable = true)
          )
        )

        val rawFileLine = RawFileLine(
          sourceFileMetadata = remoteFile,
          logMetadata = Some(testLogMetadata),
          logContent = Some(testLogContent),
          downloadTimestamp = testTimestamp
        )
        val row = rawFileLine.toInternalRow(partialSchema)

        row should not be null
        row.numFields shouldBe 2
        row.getString(0) shouldBe testLogMetadata
        row.getBinary(1) shouldBe testLogContent
      }

      it("should handle schema with error only") {
        val partialSchema = StructType(
          Seq(
            StructField(RemoteFileFormat.ERROR, StringType, nullable = true)
          )
        )

        val rawFileLine = RawFileLine(remoteFile, downloadTimestamp = testTimestamp, error = Some(testError))
        val row         = rawFileLine.toInternalRow(partialSchema)

        row should not be null
        row.numFields shouldBe 1
        row.getString(0) shouldBe testError
      }
    }

    describe("convertSourceFileToStruct") {

      it("should correctly convert timestamp to microseconds") {
        val timestampMillis = 1609459200000L // 2021-01-01 00:00:00
        val timestamp       = new Timestamp(timestampMillis)
        val file            = RemoteFile(name = "test.log", fetchedAt = timestamp)
        val rawFileLine     = RawFileLine(file, downloadTimestamp = timestamp)

        val sourceFileSchema = StructType(
          Seq(
            StructField(RemoteFileFormat.SOURCE_FILE_NAME, StringType, nullable = false),
            StructField(RemoteFileFormat.SOURCE_FILE_FETCHED_AT, TimestampType, nullable = false)
          )
        )

        val schema = StructType(
          Seq(
            StructField(RemoteFileFormat.SOURCE_FILE, sourceFileSchema, nullable = false)
          )
        )

        val row              = rawFileLine.toInternalRow(schema)
        val sourceFileStruct = row.getStruct(0, 2)

        // Verify microseconds conversion
        sourceFileStruct.getLong(1) shouldBe timestampMillis * 1000L
      }

      it("should handle null optional HTTP metadata fields") {
        val file        = RemoteFile(name = "test.log", fetchedAt = testTimestamp)
        val rawFileLine = RawFileLine(file, downloadTimestamp = testTimestamp)

        val row              = rawFileLine.toInternalRow(RemoteFileFormat.SCHEMA)
        val sourceFileStruct = row.getStruct(0, 6)

        // Required fields should have values
        sourceFileStruct.getString(0) shouldBe "test.log"
        sourceFileStruct.getLong(1) shouldBe testTimestamp.getTime * 1000L

        // Optional fields should be null
        sourceFileStruct.isNullAt(2) shouldBe true // fileSize
        sourceFileStruct.isNullAt(3) shouldBe true // lastModified
        sourceFileStruct.isNullAt(4) shouldBe true // etag
        sourceFileStruct.isNullAt(5) shouldBe true // requestId
      }
    }

    describe("with empty schema") {

      it("should handle empty schema") {
        val emptySchema = StructType(Seq.empty)
        val rawFileLine = RawFileLine(remoteFile, logMetadata = Some(testLogMetadata), downloadTimestamp = testTimestamp)
        val row         = rawFileLine.toInternalRow(emptySchema)

        row should not be null
        row.numFields shouldBe 0
      }
    }
  }
}
// scalafix:on DisableSyntax.null
