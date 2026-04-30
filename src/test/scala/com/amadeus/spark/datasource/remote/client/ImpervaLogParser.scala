package com.amadeus.spark.datasource.remote.client

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}

/**
 * Utility object for parsing Imperva log files.
 *
 * Imperva log files have the structure:
 * - UTF-8 encoded header with key:value pairs (one per line)
 * - Separator: b"|==|\n"
 * - Encrypted binary log content
 */
object ImpervaLogParser {

  /** The separator between header and encrypted content */
  val SEPARATOR: Array[Byte] = Array('|'.toByte, '='.toByte, '='.toByte, '|'.toByte, '\n'.toByte)

  /**
   * Result of parsing an Imperva log file.
   *
   * @param logMetadata   parsed metadata as JSON string, or None if split failed
   * @param logContent    encrypted log content, or None if split failed
   * @param rawFileBinary the complete raw file if split failed
   * @param error         error message if parsing failed
   */
  case class ParseResult(
      logMetadata: Option[String],
      logContent: Option[Array[Byte]],
      rawFileBinary: Option[Array[Byte]],
      error: Option[String]
  )

  /**
   * Parses an Imperva log file binary.
   *
   * Attempts to split the file by b"|==|\n" separator:
   * - If successful: parses header to JSON and extracts log content
   * - If failed: stores entire file in rawFileBinary with error message
   *
   * @param fileBytes the complete log file as byte array
   * @return ParseResult with parsed components or error
   */
  def parse(fileBytes: Array[Byte]): ParseResult = {
    findSeparator(fileBytes) match {
      case Some(separatorIndex) =>
        // Split succeeded
        val headerBytes = fileBytes.slice(0, separatorIndex)
        val contentBytes = fileBytes.slice(separatorIndex + SEPARATOR.length, fileBytes.length)

        parseHeader(headerBytes) match {
          case Success(jsonString) =>
            ParseResult(
              logMetadata = Some(jsonString),
              logContent = Some(contentBytes),
              rawFileBinary = None,
              error = None
            )
          case Failure(e) =>
            // Header parsing failed, but we found the separator
            ParseResult(
              logMetadata = None,
              logContent = Some(contentBytes),
              rawFileBinary = Some(headerBytes),
              error = Some(s"Failed to parse header: ${e.getMessage}")
            )
        }

      case None =>
        // Split failed - store complete file as raw binary
        ParseResult(
          logMetadata = None,
          logContent = None,
          rawFileBinary = Some(fileBytes),
          error = Some("Failed to find separator |==| in file")
        )
    }
  }

  /**
   * Finds the index of the separator in the byte array.
   *
   * @param bytes the byte array to search
   * @return Some(index) if found, None otherwise
   */
  private def findSeparator(bytes: Array[Byte]): Option[Int] = {
    val maxIndex = bytes.length - SEPARATOR.length
    var i = 0
    while (i <= maxIndex) {
      if (bytes(i) == SEPARATOR(0) &&
          bytes(i + 1) == SEPARATOR(1) &&
          bytes(i + 2) == SEPARATOR(2) &&
          bytes(i + 3) == SEPARATOR(3) &&
          bytes(i + 4) == SEPARATOR(4)) {
        return Some(i)
      }
      i += 1
    }
    None
  }

  /**
   * Parses the header bytes to a JSON string.
   *
   * Expected format:
   * ```
   * accountId:1147454
   * configId:3088
   * checksum:390867c2c27985a7b124c8676c00137c
   * format:LEEF
   * startTime:1765877536110
   * endTime:1765877536170
   * ```
   *
   * @param headerBytes UTF-8 encoded header bytes
   * @return Success(jsonString) or Failure(exception)
   */
  private def parseHeader(headerBytes: Array[Byte]): Try[String] = Try {
    val headerText = new String(headerBytes, "UTF-8")
    val lines = headerText.split("\n").map(_.trim).filter(_.nonEmpty)

    val fields = lines.flatMap { line =>
      val parts = line.split(":", 2)
      if (parts.length == 2) {
        val key = parts(0).trim
        val value = parts(1).trim
        Some(key -> convertValue(key, value))
      } else {
        None
      }
    }.toMap

    // Convert to JSON string
    compact(render(Extraction.decompose(fields)(DefaultFormats)))
  }

  /**
   * Converts string values to appropriate types for JSON serialization.
   *
   * @param key   the field key
   * @param value the string value
   * @return typed value (Long for numeric fields, String otherwise)
   */
  private def convertValue(key: String, value: String): Any = {
    key match {
      case "accountId" | "configId" | "publicKeyId" | "startTime" | "endTime" =>
        // Try to parse as number
        Try(value.toLong).getOrElse(value)
      case _ =>
        value
    }
  }
}
