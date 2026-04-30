package com.amadeus.spark.datasource.remote.client

import java.sql.Timestamp

/**
 * Represents a file fetched from a remote endpoint with HTTP response metadata.
 *
 * @param name         the name of the file
 * @param fetchedAt    the timestamp when the file was fetched
 * @param fileSize     file size in bytes from Content-Length header
 * @param lastModified last modified timestamp from Last-Modified header
 * @param etag         ETag from HTTP response
 * @param requestId    request ID from HTTP response (x-amz-request-id)
 */
case class RemoteFile(
    name: String,
    fetchedAt: Timestamp,
    fileSize: Option[Long] = None,
    lastModified: Option[Timestamp] = None,
    etag: Option[String] = None,
    requestId: Option[String] = None
)
