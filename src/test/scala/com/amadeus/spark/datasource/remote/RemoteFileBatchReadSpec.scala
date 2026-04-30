package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.SparkTestBase
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * E2E test for batch reading with the remote file data source.
 */
class RemoteFileBatchReadSpec extends AnyFunSpec with Matchers with SparkTestBase {

  describe("RemoteFile Batch Read") {

    it("should read data in batch mode with mocked client") {
      // Create DataFrame using the data source
      val df = spark.read
        .format("rest-file")
        .option("uri", "http://localhost:5000")
        .option("remoteClient", "mock")
        .load()

      // Collect results
      val rows = df.collect()

      // Verify we got data from the mock files
      rows.length should be > 0

      // Verify schema - updated for new file-based design
      df.schema.fieldNames should contain("logMetadata")
      df.schema.fieldNames should contain("logContent")
      df.schema.fieldNames should contain("sourceFile")
    }
  }
}
