package com.clientregistrytest

import com.amadeus.spark.datasource.remote.client.{DownloadResult, RemoteFile, RemoteFileClient}

class MockForExternalFailSecTestsRemoteFileClient extends RemoteFileClient {

  /**
   * Lists available log files from the remote endpoint.
   *
   * @return sequence of [[RemoteFile]] representing available log files
   */
  override def listLogFiles(): Seq[RemoteFile] = Seq.empty

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
  override def shortName(): String = "mock-fail-test"
}
