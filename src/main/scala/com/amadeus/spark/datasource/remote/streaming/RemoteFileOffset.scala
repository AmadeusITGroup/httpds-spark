package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.{Serializer, Utils}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.Offset

import scala.language.implicitConversions

/**
 * Represents an offset in the remote file stream.
 *
 * The offset tracks which log files have been processed. Since remote
 * generates new immutable log files over time, the offset is essentially
 * a list of file IDs that have been committed.
 *
 * @param indexFiles set of file IDs that have been processed
 */
case class RemoteFileOffset(indexFiles: Set[String], createdAt: Long) extends Offset {

  override def json(): String = {
    RemoteFileOffset.toJson(this)
  }

  /**
   * Computes the files to process between this offset and an end offset.
   * Returns files that are in the end offset but not in this offset.
   */
  def filesBetween(end: RemoteFileOffset): Set[String] = {
    end.indexFiles -- this.indexFiles
  }

  /**
   * Two offsets are equal if they have the same file list, regardless of createdAt timestamp.
   */
  override def equals(obj: Any): Boolean = obj match {
    case that: RemoteFileOffset => this.indexFiles == that.indexFiles
    case _                      => false
  }

  /**
   * Hash code is based only on the file list, not the timestamp.
   */
  override def hashCode(): Int = indexFiles.hashCode()
}

/**
 * Companion object for IncapsulaOffset serialization.
 */
private object RemoteFileOffset extends Logging {

  private lazy val objectMapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  /**
   * Initial offset (no files processed).
   */
  val INITIAL: RemoteFileOffset = RemoteFileOffset(Set.empty, 0L)

  /**
   * Creates an offset from multiple files.
   */
  def fromFiles(files: Set[String]): RemoteFileOffset = {
    val now = System.currentTimeMillis()
    RemoteFileOffset(files, now)
  }

  /**
   * Serializes an offset to JSON.
   * The indexFiles Set is compressed and stored as a base64 string.
   */
  private def toJson(offset: RemoteFileOffset): String = {
    implicit val setSerializer: Serializer[Serializable] = Serializer.javaSerializer[Serializable]

    val compressedFiles = Utils.compressZstd(offset.indexFiles.asInstanceOf[Serializable])
    val map: Map[String, Any] = Map(
      "createdAt"  -> offset.createdAt,
      "indexFiles" -> compressedFiles.b64
    )
    objectMapper.writeValueAsString(map)
  }

  /**
   * Deserializes an offset from JSON.
   * The indexFiles Set is decompressed from a base64 string.
   */
  def fromJson(json: String): RemoteFileOffset = {
    val node = objectMapper.readTree(json)

    val indexFiles = {
      implicit val setSerializer: Serializer[Serializable] = Serializer.javaSerializer[Serializable]
      val b64String                                        = node.get("indexFiles").asText()
      val compressed                                       = Utils.compressedDataFromB64[Serializable](b64String)
      compressed.decompress().asInstanceOf[Set[String]]
    }

    val createdAt = node.get("createdAt").asLong()

    RemoteFileOffset(indexFiles, createdAt)
  }

  /**
   * Converts a generic Offset to RemoteFileOffset.
   */
  implicit def convert(offset: Offset): RemoteFileOffset = {
    offset match {
      case off: RemoteFileOffset => off
      case _ =>
        logWarning("Converting unknown Offset type to RemoteFileOffset via JSON serialization")
        fromJson(offset.json())
    }
  }
}
