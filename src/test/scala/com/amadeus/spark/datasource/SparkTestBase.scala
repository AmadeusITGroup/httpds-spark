package com.amadeus.spark.datasource

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Base trait for Spark tests.
 * Provides a local SparkSession for testing.
 */
trait SparkTestBase extends BeforeAndAfterAll { this: Suite =>

  // Mutable SparkSession required for lazy test base initialization
  // scalafix:off DisableSyntax.var
  @transient protected var spark: SparkSession = _
  // scalafix:on DisableSyntax.var

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
      // Null check required for BeforeAndAfterAll pattern — SparkSession may not be initialized if beforeAll failed
      // scalafix:off DisableSyntax.null
      if (spark != null) {
        // scalafix:on DisableSyntax.null
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }
}
