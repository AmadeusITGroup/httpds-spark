package com.amadeus.spark.datasource.remote

import org.apache.spark.sql.connector.read.InputPartition

/**
 * Input partition for a set of files to download from remote endpoint.
 *
 * @param files The list of file URLs to download in this partition.
 */
case class RemoteFileInputPartition(files: Seq[String]) extends InputPartition {}
