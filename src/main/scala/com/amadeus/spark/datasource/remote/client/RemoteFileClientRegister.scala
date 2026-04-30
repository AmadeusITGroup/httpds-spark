package com.amadeus.spark.datasource.remote.client

/**
 * Trait for registering remote file clients with a short name.
 */
trait RemoteFileClientRegister {

  /**
   * The string that represents the format that this client provider uses.
   * This is overridden by children to provide a nice alias for the clients.
   * For example:
   *   override def shortName(): String = "rest-client"
   */
  def shortName(): String
}
