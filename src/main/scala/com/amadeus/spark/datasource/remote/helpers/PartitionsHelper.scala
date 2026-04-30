package com.amadeus.spark.datasource.remote.helpers

import com.amadeus.spark.datasource.remote.RemoteFileInputPartition
import org.apache.spark.internal.Logging

import java.util.UUID.randomUUID
import scala.collection.mutable

/**
 * Helper trait for creating input partitions for remote file streaming.
 *
 * This trait provides functionality to distribute a set of file names into a specified number of partitions.
 * The files are sorted alphanumerically and distributed in a round-robin fashion to ensure balanced partitions.
 *
 * The resulting partitions are represented as RemoteFileInputPartition instances, which contain the list of files assigned to each partition.
 */
trait PartitionsHelper extends Logging {

  /** Unique identifier for this instance, used for logging to distinguish between multiple instances in the same application. */
  protected val instanceId: String = randomUUID().toString.take(8)

  /**
   * Creates input partitions based on the number of files and desired partitions.
   * Files are sorted alphanumerically before distribution.
   *
   * @param numPartitions number of partitions to create
   * @param fileNames set of file names to distribute
   * @return array of input partitions
   */
  protected def createPartitions(numPartitions: Int, fileNames: Seq[String]): Array[RemoteFileInputPartition] = {
    val sortedFileNames = fileNames.sorted

    logDebug(s"[STREAM-$instanceId]   Distributing ${sortedFileNames.size} files across $numPartitions partitions using round-robin")

    implicit val partitionBuilders: Array[mutable.Builder[String, Seq[String]]] = Array.fill(numPartitions)(Seq.newBuilder[String])

    distributeFilesInPartitions(sortedFileNames, numPartitions)

    buildFinalPartitions
  }

  /**
   * Distributes sorted file names into partition builders in round-robin fashion.
   *
   * @param sortedFileNames sequence of sorted file names to distribute
   * @param numPartitions number of partitions to distribute into
   * @param partitionBuilders array of mutable builders for each partition
   */
  private def distributeFilesInPartitions(sortedFileNames: Seq[String], numPartitions: Int)(implicit partitionBuilders: Array[mutable.Builder[String, Seq[String]]]): Unit = {
    sortedFileNames.zipWithIndex.foreach { case (fileName, idx) =>
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
