package com.amadeus.spark.datasource.remote

import com.github.luben.zstd.{ZstdInputStream, ZstdOutputStream}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

case class CompressedData[T](bytes: Array[Byte], serializer: Serializer[T]) {
  lazy val b64: String = Base64.getEncoder.encodeToString(bytes)

  def decompress(): T = Utils.decompressZstd(this)
}

trait Serializer[T] {
  def serialize(value: T): Array[Byte]
  def deserialize(bytes: Array[Byte]): T
}

object Serializer {
  implicit val stringSerializer: Serializer[String] = new Serializer[String] {
    def serialize(value: String): Array[Byte]   = value.getBytes("UTF-8")
    def deserialize(bytes: Array[Byte]): String = new String(bytes, "UTF-8")
  }

  def javaSerializer[T <: java.io.Serializable]: Serializer[T] = new Serializer[T] {
    def serialize(value: T): Array[Byte] = {
      val byteStream   = new ByteArrayOutputStream()
      val objectStream = new ObjectOutputStream(byteStream)
      objectStream.writeObject(value)
      objectStream.close()
      byteStream.toByteArray
    }

    def deserialize(bytes: Array[Byte]): T = {
      val byteStream   = new ByteArrayInputStream(bytes)
      val objectStream = new ObjectInputStream(byteStream)
      val result       =
        // scalafix:off DisableSyntax.asInstanceOf
        objectStream.readObject().asInstanceOf[T]
      // scalafix:on DisableSyntax.asInstanceOf
      objectStream.close()
      result
    }
  }
}

object Utils {

  def compressedDataFromB64[T](b64: String)(implicit serializer: Serializer[T]): CompressedData[T] = {
    CompressedData(Base64.getDecoder.decode(b64), serializer)
  }

  /**
   * Compress data using Zstandard compression.
   *
   * @param value The value to compress
   * @param serializer The serializer for type T
   * @return CompressedData containing the compressed bytes
   */
  def compressZstd[T](value: T)(implicit serializer: Serializer[T]): CompressedData[T] = {
    val data       = serializer.serialize(value)
    val byteStream = new ByteArrayOutputStream()
    val zstdStream = new ZstdOutputStream(byteStream)
    try {
      zstdStream.write(data)
      zstdStream.flush()
    } finally {
      zstdStream.close()
      byteStream.close()
    }
    CompressedData(byteStream.toByteArray, serializer)
  }

  /**
   * Decompress data using Zstandard compression.
   *
   * @param compressed The compressed data
   * @return The decompressed value
   */
  def decompressZstd[T](compressed: CompressedData[T]): T = {
    val bytes = decompressZstdToBytes(compressed.bytes)
    compressed.serializer.deserialize(bytes)
  }

  /**
   * Decompress bytes using Zstandard compression.
   *
   * @param bytes The compressed bytes
   * @return The decompressed bytes
   */
  def decompressZstdToBytes(bytes: Array[Byte]): Array[Byte] = {
    val byteStream = new ByteArrayInputStream(bytes)
    val zstdStream = new ZstdInputStream(byteStream)
    try {
      zstdStream.readAllBytes()
    } finally {
      zstdStream.close()
      byteStream.close()
    }
  }
}
