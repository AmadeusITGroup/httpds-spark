package com.amadeus.spark.datasource.remote.client

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.collection.mutable

/**
 * Mock implementation of RemoteFileClient for testing.
 * Returns predefined data without making actual HTTP calls.
 */
class MockRemoteFileClient extends RemoteFileClient {

  // Mutable state required for mock client options tracking in tests
  // scalafix:off DisableSyntax.var
  private[client] var options: RemoteFileDataSourceOptions = _
  // scalafix:on DisableSyntax.var

  override def init(opts: RemoteFileDataSourceOptions): Unit = {
    this.options = opts
  }

  // Predefined mock data
  private val mockFiles = mutable.LinkedHashMap(
    "file1.log" -> """{"timestamp":"2024-01-01T10:00:00Z","message":"Log entry 1", "metadata":"file1 mtd"}""",
    "file2.log" -> """{"timestamp":"2024-01-01T10:01:00Z","message":"Log entry 2", "metadata":"file2 mtd"}""",
    "file3.log" -> """{"timestamp":"2024-01-01T10:02:00Z","message":"Log entry 3", "metadata":"file3 mtd"}"""
  )

  override def listLogFiles(): Seq[RemoteFile] = {
    mockFiles.keys.map(name => RemoteFile(name, Timestamp.valueOf(LocalDateTime.now()))).toSeq
  }

  override def downloadLogFileWithResult(filename: String): DownloadResult = {

    mockFiles.get(filename) match {
      case Some(content) =>
        val timestamp  = Timestamp.valueOf(LocalDateTime.now())
        val remoteFile = RemoteFile(filename, timestamp)
        val fileBytes  = content.getBytes("UTF-8")

        val parseResult = TestLogParser.parse(fileBytes)

        // Create single RawFileLine for the entire file
        val rawFileLine = RawFileLine(
          sourceFileMetadata = remoteFile,
          logMetadata = parseResult.logMetadata,
          logContent = parseResult.logContent,
          rawFileBinary = parseResult.rawFileBinary,
          downloadTimestamp = timestamp,
          error = parseResult.error
        )

        DownloadSuccess(Iterator.single(rawFileLine), DownloadMetrics(1, fileBytes.length))
      case None =>
        val timestamp  = new java.sql.Timestamp(System.currentTimeMillis())
        val remoteFile = RemoteFile(filename, timestamp)
        DownloadFailure(RawFileLine(remoteFile, downloadTimestamp = timestamp, error = Some(s"File not found: $filename")))
    }
  }

  override def close(): Unit = {
    // No resources to close in mock
  }

  override def shortName(): String = "mock"

  /**
   * Add a mock file for testing.
   */
  def addMockFile(filename: String, content: String): Unit = {
    val _ = mockFiles.put(filename, content)
  }

  /**
   * Clear all mock files.
   */
  def clearMockFiles(): Unit = {
    mockFiles.clear()
  }
}
