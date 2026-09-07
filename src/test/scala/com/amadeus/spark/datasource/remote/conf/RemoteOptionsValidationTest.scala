package com.amadeus.spark.datasource.remote.conf

import com.amadeus.spark.datasource.remote.{RemoteFileFormat, RemoteFileInputPartition, RemoteFilePartitionReaderFactory}
import com.amadeus.spark.datasource.remote.read.RemoteFileBatch
import com.amadeus.spark.datasource.remote.streaming.RemoteMicroBatchStream
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class RemoteOptionsValidationTest extends AnyFunSpec with Matchers {
  private val base = Map("uri" -> "http://localhost:5000", "remoteClient" -> "not-a-real-client")
  private val invalid = Seq(
    "partitions"           -> "0",
    "maxRetries"           -> "-1",
    "maxFilesPerTrigger"   -> "0",
    "asyncPrefetchSize"    -> "0",
    "asyncPrefetchSize"    -> "-1",
    "asyncDownloadThreads" -> "0",
    "asyncDownloadThreads" -> "-1",
    "connectTimeout"       -> "0s",
    "connectTimeout"       -> "-1s",
    "connectTimeout"       -> "1ns",
    "connectTimeout"       -> "Inf",
    "readTimeout"          -> "0s",
    "readTimeout"          -> "-1s",
    "readTimeout"          -> "Inf",
    "pollInterval"         -> "-1s",
    "pollInterval"         -> "Inf",
    "retryDelay"           -> "-1s",
    "retryDelay"           -> "Inf",
    "remoteClient"         -> "",
    "remoteClient"         -> "   ",
    "uri"                  -> "   "
  )

  private val entryPoints: Seq[(String, CaseInsensitiveStringMap => Unit)] = Seq(
    "parser"    -> ((options: CaseInsensitiveStringMap) => { val _ = RemoteFileDataSourceOptions.fromMap(options) }),
    "batch"     -> ((options: CaseInsensitiveStringMap) => { val _ = new RemoteFileBatch(RemoteFileFormat.SCHEMA, options) }),
    "streaming" -> ((options: CaseInsensitiveStringMap) => { val _ = new RemoteMicroBatchStream(RemoteFileFormat.SCHEMA, options) }),
    "reader factory" -> ((options: CaseInsensitiveStringMap) => {
      val factory = new RemoteFilePartitionReaderFactory(RemoteFileFormat.SCHEMA, options)
      factory.createReader(RemoteFileInputPartition(Seq.empty)).close()
    })
  )

  for ((entryPoint, create) <- entryPoints; (key, value) <- invalid) {
    it(s"rejects $key=$value at the $entryPoint before creating a client") {
      val options = new CaseInsensitiveStringMap((base + (key -> value)).asJava)
      intercept[IllegalArgumentException](create(options)).getMessage should include(key)
    }
  }

  for ((entryPoint, create) <- entryPoints) {
    it(s"reports a missing remoteClient at the $entryPoint without a null dereference") {
      val options = new CaseInsensitiveStringMap((base - "remoteClient").asJava)
      intercept[IllegalArgumentException](create(options)).getMessage should include("remoteClient")
    }
  }

  it("accepts zero polling/retry delays and minimum positive resource settings") {
    val options = new CaseInsensitiveStringMap(
      (base ++ Map(
        "remoteClient"         -> "mock",
        "connectTimeout"       -> "1ms",
        "readTimeout"          -> "1ms",
        "pollInterval"         -> "0s",
        "retryDelay"           -> "0s",
        "maxRetries"           -> "0",
        "asyncPrefetchSize"    -> "1",
        "asyncDownloadThreads" -> "1"
      )).asJava
    )
    entryPoints.foreach { case (_, create) => noException should be thrownBy create(options) }
  }
}
