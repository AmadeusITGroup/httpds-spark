package com.amadeus.spark.datasource.remote.streaming

/**
 * Workload definition for Incapsula streaming.
 *
 * @param filesToProcess sequence of file names to process in this workload
 * @param fullFileList   set of all available file names
 */
case class Workload(filesToProcess: Seq[String], fullFileList: Set[String])
