package com.amadeus.spark.datasource.remote.client

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.internal.Logging
import scala.language.existentials

import scala.util.Try

/**
 * Client interface for interacting with remote-based file sources.
 */
trait RemoteFileClient extends RemoteFileClientRegister with Serializable with Logging {

  /**
   * Initializes the client with the given options.
   *
   * Override if the implementation class does not provide a constructor with RestFileDataSourceOptions.
   *
   * @param options configuration options for the client
   */
  def init(options: RemoteFileDataSourceOptions): Unit = {}

  /**
   * Lists available log files from the remote endpoint.
   *
   * @return sequence of RestFile representing available log files
   */
  def listLogFiles(): Seq[RemoteFile]

  /**
   * Lists and sorts available log files from the remote endpoint.
   * If the client implements FileSorter, applies custom sorting logic.
   * Otherwise, sorts by fetchedAt timestamp (default behavior).
   *
   * @return sequence of sorted RemoteFile
   */
  def listAndSortLogFiles(): Seq[RemoteFile] = {
    val files = listLogFiles()
    this match {
      case sorter: FileSorter =>
        logDebug(s"Client implements FileSorter, applying custom sort to ${files.size} files")
        sorter.sortFiles(files)
      case _ =>
        logDebug(s"Client does not implement FileSorter, sorting ${files.size} files by fetchedAt")
        files.sortBy(_.fetchedAt.getTime)
    }
  }

  /**
   * Downloads a log file and returns the result with metadata.
   *
   * @param filename name of the log file to download
   * @return DownloadResult representing the outcome of the download operation
   */
  def downloadLogFileWithResult(filename: String): DownloadResult

  /**
   * Closes the client and releases any resources.
   */
  def close(): Unit
}

object RemoteFileClient extends Logging {

  /**
   * Creates and initializes the RestFileClient based on the configuration.
   *
   * @param config configuration options
   * @return initialized RestFileClient
   */
  def from(config: RemoteFileDataSourceOptions): RemoteFileClient = {
    val clientClass          = ClientRegistry.lookupDataSource(config.remoteClient)
    val optionArgConstructor = Try(clientClass.getDeclaredConstructor(classOf[RemoteFileDataSourceOptions])).toOption

    if (optionArgConstructor.isDefined) {
      logDebug("Instantiating remote file client with RestFileDataSourceOptions constructor")
      return optionArgConstructor.get
        .newInstance(config)
        .asInstanceOf[RemoteFileClient]
    }

    logWarning(s"No constructor with RestFileDataSourceOptions found for ${config.remoteClient}, trying empty constructor")
    val client = clientClass.getDeclaredConstructor().newInstance().asInstanceOf[RemoteFileClient]
    client.init(config)
    client
  }
}
