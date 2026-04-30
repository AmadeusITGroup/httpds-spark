package com.amadeus.spark.datasource.remote.client

import org.apache.spark.internal.Logging

import java.util.ServiceLoader
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * Registry for remote file clients.
 * Uses Java's ServiceLoader to discover implementations of RestFileClientRegister.
 */
object ClientRegistry extends Logging {

  /** Prefixes to identify internal clients (open-source framework + private imperva implementation). */
  private val INTERNAL_CLIENT_PREFIXES: Set[String] = Set(
    "com.amadeus.spark.datasource",
    "com.imperva.spark.datasource"
  )

  /**
   * Looks up the data source class based on the provided short name or fully qualified class name.
   *
   * @param provider the short name or fully qualified class name of the data source
   * @return the Class object of the data source
   */
  def lookupDataSource(provider: String): Class[_] = {
    implicit val loader: ClassLoader = classLoader
    val serviceLoader                = ServiceLoader.load(classOf[RemoteFileClientRegister], loader)

    serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(provider)).toList match {
      case Nil         => loadByClassName(provider)
      case head :: Nil => head.getClass
      case sources     => loadFromMultipleClients(provider, sources)
    }
  }

  /**
   *  Loads a class by its fully qualified class name.
   *
   * @param className the fully qualified name of the class to load
   * @param loader    the class loader to use
   * @return the loaded Class object
   */
  private def loadByClassName(className: String)(implicit loader: ClassLoader): Class[_] = {
    logWarning(s"Short name not found for $className in service loader, attempting to load by class name.")
    loader.loadClass(className)
  }

  /**
   * Handles the case where multiple clients are found for the same provider name.
   * If one of the clients is an internal client, it is selected over external ones.
   * Otherwise, an exception is thrown to indicate ambiguity.
   *
   * @param provider the short name of the provider
   * @param clients  the list of RestFileClientRegister instances found
   * @return the Class object of the selected client
   */
  private def loadFromMultipleClients(provider: String, clients: List[RemoteFileClientRegister]): Class[_] = {
    val clientNames     = clients.map(_.getClass.getName).sorted
    val internalClients = clients.filter(c => INTERNAL_CLIENT_PREFIXES.exists(c.getClass.getName.startsWith))

    if (internalClients.size == 1) {
      logWarning(s"Multiple data sources found for the same short name $provider.")

      val externalSources = clientNames.filterNot(name => INTERNAL_CLIENT_PREFIXES.exists(name.startsWith))
      val internalClient  = internalClients.head.getClass

      logWarning(s"Selecting internal source: ${internalClient.getName} over external sources: ${externalSources.mkString(", ")}")
      return internalClient
    }

    throw new IllegalArgumentException(
      s"Multiple data sources found for the same short name $provider: ${clientNames.mkString(", ")}. Please specify the fully qualified class name to avoid ambiguity."
    )
  }

  /** Retrieves the appropriate class loader. */
  private def classLoader: ClassLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
}
