package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.client._
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import com.amadeus.spark.datasource.remote.helpers.RemoteFilePartitionReaderHelper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

/**
 * Partition reader for remote file input partitions.
 *
 * This reader downloads files from a remote endpoint and converts them into InternalRow format.
 *
 * @param schema    the schema of the data
 * @param partition the input partition containing files to read
 * @param options   configuration options
 */
class RemoteFilePartitionReader(schema: StructType, partition: RemoteFileInputPartition, options: RemoteFileDataSourceOptions)
    extends RemoteFilePartitionReaderHelper(schema, partition)
    with Logging {

  /** remote client for downloading files. */
  @transient private lazy val client: RemoteFileClient = RemoteFileClient.from(options)

  /** Current file index being processed. */
  protected var currentFileIndex: Int = 0

  /**
   * Downloads the next file if the current iterator is exhausted.
   *
   *  @return true if a new file was downloaded and currentIterator is updated, false if no more files
   */
  override protected def downloadNextFile(): Boolean = {
    while (!currentIterator.hasNext) {
      // Move to next file
      if (currentFileIndex >= partition.files.size) {
        logInfo(s"[PARTITION-$partition] All files processed - ending iteration")
        return false
      }

      val filename = partition.files(currentFileIndex)
      currentFileIndex += 1

      logDebug(s"[PARTITION-$partition] Processing file $currentFileIndex/${partition.files.size}: $filename")
      logDebug(s"[PARTITION-$partition]   Thread: ${Thread.currentThread().getName}")

      val downloadResult = client.downloadLogFileWithResult(filename)

      processDownloadedFile(filename, downloadResult)
    }

    true
  }

  /**
   * Closes the client and releases resources.
   */
  override protected def closeClient(): Unit = client.close()

}
