package com.amadeus.spark.datasource

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Base trait for Spark tests.
 * Provides a local SparkSession for testing.
 */
trait SparkTestBase extends BeforeAndAfterAll { this: Suite =>

  @transient protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }
}
