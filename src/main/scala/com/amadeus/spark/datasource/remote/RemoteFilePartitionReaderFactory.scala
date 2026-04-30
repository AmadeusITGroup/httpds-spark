package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * Factory for creating partition readers for remote file input partitions.
 *
 * @param schema  the schema of the data
 * @param options configuration options
 */
class RemoteFilePartitionReaderFactory(schema: StructType, options: CaseInsensitiveStringMap) extends PartitionReaderFactory with Logging with Serializable {

  /** Store options as serializable Java Map */
  private val optionsMap: util.Map[String, String] = options.asCaseSensitiveMap()

  /**
   * Creates a partition reader for the given input partition.
   *
   * Selects between AsyncRemoteFilePartitionReader and RemoteFilePartitionReader
   * based on the asyncDownloads configuration option.
   *
   * For async downloads:
   * 1. Gets shared ExecutionContext from ExecutorAsyncResources
   * 2. Creates client and initializes async support if available
   * 3. Creates AsyncRemoteFilePartitionReader with configured client
   *
   * @param partition the input partition
   * @return a partition reader for reading data from the partition
   */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val restFilePartition = partition.asInstanceOf[RemoteFileInputPartition]
    val parsedOptions     = RemoteFileDataSourceOptions.fromMap(optionsMap)

    // Choose reader based on async downloads configuration
    if (parsedOptions.asyncDownloads) {
      AsyncRemoteFilePartitionReader(
        schema = schema,
        optionsMap = optionsMap,
        parsedOptions = parsedOptions,
        restFilePartition = restFilePartition
      )

    } else {
      logDebug(s"[READER-FACTORY] Creating standard RemoteFilePartitionReader for partition ${restFilePartition}")
      new RemoteFilePartitionReader(
        schema = schema,
        partition = restFilePartition,
        options = parsedOptions
      )
    }
  }

  /**
   * Indicates whether columnar reads are supported.
   *
   * @param partition the input partition
   * @return false as columnar reads are not supported
   */
  override def supportColumnarReads(partition: InputPartition): Boolean = false
}
