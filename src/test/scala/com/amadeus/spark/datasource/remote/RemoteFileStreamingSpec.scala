package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.SparkTestBase
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/**
 * E2E test for streaming reads with the remote file data source.
 */
class RemoteFileStreamingSpec extends AnyFunSpec with Matchers with SparkTestBase {

  describe("RemoteFile Streaming Read") {

    it("should process micro-batch with mocked client") {
      // Create a temporary checkpoint directory
      val checkpointDir = Files.createTempDirectory("checkpoint").toString

      try {
        // Create streaming DataFrame
        val streamDf = spark.readStream
          .format("rest-file")
          .option("uri", "http://localhost:5000")
          .option("remoteClient", "mock")
          .option("maxFilesPerTrigger", "2")
          .load()

        // Start a streaming query that writes to memory
        val query = streamDf.writeStream
          .format("memory")
          .queryName("test_query")
          .trigger(Trigger.AvailableNow())
          .option("checkpointLocation", checkpointDir)
          .start()

        // Wait for the query to process
        query.processAllAvailable()

        // Read from the memory sink
        val resultDf = spark.sql("SELECT * FROM test_query")
        val rows     = resultDf.collect()

        // Verify we got data
        rows.length should be > 0

        // Verify schema
        resultDf.schema.fieldNames should contain("logMetadata")
        resultDf.schema.fieldNames should contain("logContent")
        resultDf.schema.fieldNames should contain("sourceFile")

        // Stop the query
        query.stop()
      } finally {
        // Clean up checkpoint directory
        val checkpointPath = new java.io.File(checkpointDir)
        if (checkpointPath.exists()) {
          deleteRecursively(checkpointPath)
        }
      }
    }
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
