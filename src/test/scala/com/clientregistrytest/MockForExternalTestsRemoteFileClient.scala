package com.clientregistrytest

import com.amadeus.spark.datasource.remote.client.{DownloadResult, RemoteFile, RemoteFileClient}

import java.sql.Timestamp

class MockForExternalTestsRemoteFileClient extends RemoteFileClient {

  /**
   * Lists available log files from the remote endpoint.
   *
   * @return sequence of RestFile representing available log files
   */
  override def listLogFiles(): Seq[RemoteFile] = Seq(
    RemoteFile(name = "Log entry 1", fetchedAt = Timestamp.valueOf("2024-01-01 10:50:00")),
    RemoteFile(name = "Log entry 2", fetchedAt = Timestamp.valueOf("2024-01-01 10:00:00")),
    RemoteFile(name = "Log entry 3", fetchedAt = Timestamp.valueOf("2024-01-01 10:28:00"))
  )

  /**
   * Downloads a log file and returns the result with metadata.
   *
   * @param filename name of the log file to download
   * @return DownloadResult representing the outcome of the download operation
   */
  override def downloadLogFileWithResult(filename: String): DownloadResult = throw new NotImplementedError()

  /**
   * Closes the client and releases any resources.
   */
  override def close(): Unit = throw new NotImplementedError()

  /**
   * The string that represents the format that this client provider uses.
   * This is overridden by children to provide a nice alias for the clients.
   * For example:
   * override def shortName(): String = "rest-client"
   */
  override def shortName(): String = "mock-test"
}
