package com.amadeus.spark.datasource.remote

import java.sql.Timestamp

/**
 * Case class matching [[RemoteFileFormat.SCHEMA]] for use in unit tests.
 */
case class RemoteFileRow(
    sourceFile: SourceFile,
    logMetadata: Option[String] = None,
    logContent: Option[Array[Byte]] = None,
    rawFileBinary: Option[Array[Byte]] = None,
    downloadTimestamp: Timestamp,
    error: Option[String] = None
)

case class SourceFile(
    name: String,
    fetchedAt: Timestamp,
    fileSize: Option[Long] = None,
    lastModified: Option[Timestamp] = None,
    etag: Option[String] = None,
    requestId: Option[String] = None
)
