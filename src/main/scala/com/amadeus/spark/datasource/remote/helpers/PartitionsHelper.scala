package com.amadeus.spark.datasource.remote.helpers

import com.amadeus.spark.datasource.remote.RemoteFileInputPartition
import org.apache.spark.internal.Logging

import java.util.UUID.randomUUID
import scala.collection.mutable

/**
 * Helper trait for creating input partitions for remote file streaming.
 *
 * This trait provides functionality to distribute a sequence of file names into a specified number of partitions
 * using round-robin distribution to keep partitions balanced.
 *
 * By default the incoming file order is preserved (e.g. the ordering produced by
 * `RemoteFileClient.listAndSortLogFiles()`, which honours a custom `FileSorter` or sorts by `fetchedAt`).
 * Callers that start from an unordered collection can opt into alphanumeric sorting via `sortByName` to
 * obtain a deterministic distribution.
 *
 * The resulting partitions are represented as RemoteFileInputPartition instances, which contain the list of files assigned to each partition.
 */
trait PartitionsHelper extends Logging {

  /** Unique identifier for this instance, used for logging to distinguish between multiple instances in the same application. */
  protected val instanceId: String = randomUUID().toString.take(8)

  /**
   * Creates input partitions based on the number of files and desired partitions.
   *
   * The incoming order of `fileNames` is preserved by default so that the ordering established by the
   * client (custom `FileSorter` or `fetchedAt`) drives the round-robin distribution. Set `sortByName` to
   * `true` to sort the file names alphanumerically first — useful when the input has no meaningful order
   * (e.g. a Set) and a deterministic distribution is required.
   *
   * @param numPartitions number of partitions to create
   * @param fileNames     sequence of file names to distribute
   * @param sortByName    when true, sort file names alphanumerically before distribution (default: false)
   * @return array of input partitions
   */
  protected def createPartitions(numPartitions: Int, fileNames: Seq[String], sortByName: Boolean = false): Array[RemoteFileInputPartition] = {
    val orderedFileNames = if (sortByName) fileNames.sorted else fileNames

    logDebug(s"[STREAM-$instanceId]   Distributing ${orderedFileNames.size} files across $numPartitions partitions using round-robin (sortByName=$sortByName)")

    implicit val partitionBuilders: Array[mutable.Builder[String, Seq[String]]] = Array.fill(numPartitions)(Seq.newBuilder[String])

    distributeFilesInPartitions(orderedFileNames, numPartitions)

    buildFinalPartitions
  }

  /**
   * Distributes the ordered file names into partition builders in round-robin fashion.
   *
   * @param orderedFileNames sequence of file names to distribute, in the order they should be assigned
   * @param numPartitions number of partitions to distribute into
   * @param partitionBuilders array of mutable builders for each partition
   */
  private def distributeFilesInPartitions(orderedFileNames: Seq[String], numPartitions: Int)(implicit partitionBuilders: Array[mutable.Builder[String, Seq[String]]]): Unit = {
    orderedFileNames.zipWithIndex.foreach { case (fileName, idx) =>
      val partitionIdx = idx % numPartitions
      partitionBuilders(partitionIdx) += fileName
    }
  }

  /**
   * Builds the final array of RemoteFileInputPartition from the partition builders.
   *
   * @param partitionBuilders array of mutable builders containing file names for each partition
   * @return array of RemoteFileInputPartition with assigned files
   */
  private def buildFinalPartitions(implicit partitionBuilders: Array[mutable.Builder[String, Seq[String]]]): Array[RemoteFileInputPartition] = {
    partitionBuilders.map(builder => {
      val filesInPartition = builder.result()
      RemoteFileInputPartition(
        files = filesInPartition
      )
    })
  }

}
