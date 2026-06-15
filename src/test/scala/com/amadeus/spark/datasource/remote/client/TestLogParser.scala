package com.amadeus.spark.datasource.remote.client

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}

/**
 */
object TestLogParser {

  case class ParseResult(
      logMetadata: Option[String],
      logContent: Option[Array[Byte]],
      rawFileBinary: Option[Array[Byte]],
      error: Option[String]
  )

  /**
   * Parses a JSON log file.
   *
   * Expects the file content to be a JSON object (e.g. {"timestamp":"...","message":"...","metadata":"..."}).
   * The "metadata" field is extracted as logMetadata, and the remaining JSON is stored as logContent bytes.
   * If parsing fails, stores entire file in rawFileBinary with an error message.
   *
   * @param fileBytes the complete log file as byte array
   * @return ParseResult with parsed components or error
   */
  def parse(fileBytes: Array[Byte]): ParseResult = {
    Try {
      val jsonString = new String(fileBytes, "UTF-8")
      val json       = org.json4s.jackson.JsonMethods.parse(jsonString)

      val metadata = json \ "metadata" match {
        case JString(s) => Some(s)
        case _          => None
      }

      // Remove metadata field; remaining JSON becomes logContent
      val contentJson  = json.removeField { case (name, _) => name == "metadata" }
      val contentBytes = compact(render(contentJson)).getBytes("UTF-8")

      ParseResult(
        logMetadata = metadata,
        logContent = Some(contentBytes),
        rawFileBinary = None,
        error = None
      )
    } match {
      case Success(result) => result
      case Failure(ex) =>
        ParseResult(
          logMetadata = None,
          logContent = None,
          rawFileBinary = Some(fileBytes),
          error = Some(s"Failed to parse JSON: ${ex.getMessage}")
        )
    }
  }

}
