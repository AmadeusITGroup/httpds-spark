package com.amadeus.spark.datasource.remote.read

import com.amadeus.spark.datasource.remote.RemoteFilePartitionReaderFactory
import com.amadeus.spark.datasource.remote.client.RemoteFileClient
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import com.amadeus.spark.datasource.remote.helpers.PartitionsHelper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Batch implementation for remote file data source.
 *
 * @param schema        the schema of the data
 * @param options       configuration options
 */
class RemoteFileBatch(schema: StructType, options: CaseInsensitiveStringMap) extends Batch with PartitionsHelper with Logging {

  /** Configuration options parsed from the provided map. */
  private val config: RemoteFileDataSourceOptions = RemoteFileDataSourceOptions.fromMap(options)

  /** remote file client for interacting with the remote endpoint. */
  private lazy val client: RemoteFileClient = RemoteFileClient.from(config)

  /**
   * Plans input partitions for parallel processing.
   *
   * This method runs on the driver and determines how the data will be split.
   * Each [[InputPartition]] becomes a Spark task that runs on an executor.
   * Files are sorted and distributed in round-robin fashion to ensure earliest
   * files start downloading across all executors simultaneously.
   *
   * Important: InputPartitions must be Serializable as they are sent to executors.
   *
   * @return an array of input partitions
   */
  override def planInputPartitions(): Array[InputPartition] = {
    logDebug(s"Planning batch read with max ${config.numPartitions} partitions")

    val files = client.listAndSortLogFiles()

    if (files.isEmpty) {
      logWarning("No log files found on server")
      // scalafix:off DisableSyntax.return
      return Array.empty
      // scalafix:on DisableSyntax.return
    }

    val numPartitions = math.min(files.size, config.numPartitions)
    logDebug(s"Creating $numPartitions partitions for ${files.size} files using round-robin distribution")

    val partitions = createPartitions(numPartitions, files.map(_.name))

    logDebug(s"Created ${partitions.length} input partitions")
    // scalafix:off DisableSyntax.asInstanceOf
    partitions.asInstanceOf[Array[InputPartition]]
    // scalafix:on DisableSyntax.asInstanceOf
  }

  /**
   * Creates a factory for partition readers.
   *
   * The factory is serialized and sent to executors, where it creates
   * actual readers for each partition.
   *
   * @return a partition reader factory
   */
  override def createReaderFactory(): PartitionReaderFactory = new RemoteFilePartitionReaderFactory(schema, options)
}
