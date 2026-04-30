package com.amadeus.spark.datasource.remote.client

/**
 * Optional trait that RemoteFileClient implementations can extend to customize
 * how files are sorted before partitioning and processing.
 *
 * If implemented, the custom sorting strategy will be applied to ensure
 * deterministic and optimal file processing order. This is particularly important
 * for round-robin partitioning where earlier files should be distributed across
 * all partitions before later files.
 *
 * Default behavior (if not implemented): Files are sorted by fetchedAt timestamp.
 * Common implementations:
 * - Sort by filename (lexicographic order)
 * - Sort by timestamp
 * - Custom domain-specific ordering
 *
 * Example implementation for Incapsula that sorts by filename:
 * {{{
 * class IncapsulaClient(...) extends RemoteFileClient with FileSorter {
 *   override def sortFiles(files: Seq[RemoteFile]): Seq[RemoteFile] = {
 *     files.sortBy(_.name)
 *   }
 * }
 * }}}
 */
trait FileSorter {

  /**
   * Sorts a sequence of remote files according to the implementation's strategy.
   *
   * @param files the unsorted sequence of files
   * @return the sorted sequence of files
   */
  def sortFiles(files: Seq[RemoteFile]): Seq[RemoteFile]
}
