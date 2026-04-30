# Imperva (Incapsula) Client for Spark Remote File DataSource

This module provides the **Imperva / Incapsula** client implementation for the `rest-file` Spark DataSource.  
It connects to the [Imperva Log Integration API](https://docs.imperva.com/bundle/cloud-application-security/page/settings/log-integration.htm) to ingest WAF (Web Application Firewall) log files into Apache Spark in both **batch** and **streaming** modes.

---

## Table of Contents

- [Overview](#overview)
- [Imperva Log File Format](#imperva-log-file-format)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Batch Mode](#batch-mode)
  - [Streaming Mode](#streaming-mode)
- [Authentication](#authentication)
- [SSL / TLS](#ssl--tls)
- [Retry Logic](#retry-logic)
- [Integration Testing with Mock Server](#integration-testing-with-mock-server)
- [Development Notes](#development-notes)

---

## Overview

The `IncapsulaClient` implements the `RemoteFileClient` interface and is registered under the short name **`imperva`**.

It interacts with two Imperva API endpoints:

| Endpoint | Purpose |
|---|---|
| `GET <uri>/logs.index` | Lists available log files (one filename per line) |
| `GET <uri>/<filename>` | Downloads a specific log file |

Files are sorted **alphanumerically by filename** to ensure deterministic processing order across all Spark executors and micro-batches.

The client also implements `AsyncRemoteFileClient`, enabling the async prefetch download pipeline provided by the framework.

---

## Imperva Log File Format

Each downloaded file is a binary blob with the following structure:

```
<UTF-8 header lines: key:value pairs>
|==|
<encrypted binary log content>
```

The framework automatically parses this format and produces the following schema columns:

| Column | Type | Description |
|---|---|---|
| `sourceFile.name` | string | Original filename from Imperva |
| `sourceFile.fetchedAt` | timestamp | When the file index was queried |
| `sourceFile.fileSize` | long | File size from `Content-Length` header |
| `sourceFile.lastModified` | timestamp | `Last-Modified` HTTP header |
| `sourceFile.etag` | string | `ETag` HTTP header |
| `sourceFile.requestId` | string | `X-Request-ID` HTTP header |
| `logMetadata` | string | JSON representation of the header key:value pairs |
| `logContent` | binary | Encrypted log content (after `\|==\|` separator) |
| `rawFileBinary` | binary | Full raw file bytes (populated only when parsing fails) |
| `downloadTimestamp` | timestamp | When the file was downloaded |
| `error` | string | Error message if download or parsing failed |

> **Note:** Decryption of `logContent` is not performed by this connector. Decryption should be applied downstream using the key provided by Imperva via `logMetadata`.

---

## Prerequisites

- An active **Imperva Cloud Security** account with Log Integration enabled.
- An **API key** and **API secret** from the Imperva management console.
- Your **Account ID** (used to build the API URI).
- Apache Spark 3.5.x cluster.
- Java 11+, Scala 2.12.

---

## Configuration

Use the following options with `.format("rest-file")`:

### Required

| Option | Description |
|---|---|
| `remoteClient` | Must be set to `imperva` |
| `uri` | Imperva Log Integration API endpoint, e.g. `https://my.imperva.com/api/audit-trail/v2/accounts/12345` |
| `apiKey` | Your Imperva API key |
| `apiSecret` | Your Imperva API secret |

### HTTP Tuning

| Option | Default | Description |
|---|---|---|
| `connectionTimeout` | `30s` | TCP connection timeout |
| `readTimeout` | `60s` | HTTP read timeout |
| `maxRetries` | `3` | Max retry attempts on transient failures |
| `retryDelay` | `1s` | Base delay between retries |
| `enableSsl` | `false` | Enable HTTPS (set to `true` in production) |
| `trustStorePath` | — | Custom trust store path (PEM / JKS) |
| `trustStorePassword` | — | Trust store password |

### Streaming

| Option | Default | Description |
|---|---|---|
| `pollInterval` | `2s` | How often to poll `/logs.index` for new files |
| `maxFilesPerTrigger` | `100` | Max files processed per micro-batch |
| `startOffset` | `earliest` | `earliest`, `latest`, or `file:<filename>` |

### Performance

| Option | Default | Description |
|---|---|---|
| `partitions` | `4` | Number of Spark partitions (batch) |
| `asyncDownloads` | `false` | Enable async prefetch download pipeline |
| `asyncPrefetchSize` | `20` | Number of files to prefetch ahead |
| `asyncDownloadThreads` | `4` | Download thread pool size per executor |

### Other

| Option | Default | Description |
|---|---|---|
| `sessionId` | — | Session identifier for test isolation (see mock server section) |

---

## Usage

### Batch Mode

```scala
val df = spark.read
  .format("rest-file")
  .option("remoteClient", "imperva")
  .option("uri", "https://my.imperva.com/api/audit-trail/v2/accounts/12345")
  .option("apiKey",    sys.env("IMPERVA_API_KEY"))
  .option("apiSecret", sys.env("IMPERVA_API_SECRET"))
  .option("enableSsl", "true")
  .option("partitions", "8")
  .option("readTimeout", "30s")
  .load()

// Show schema
df.printSchema()

// Count log files
df.select("sourceFile.name").distinct().count()

// Explore metadata
import org.apache.spark.sql.functions._
df.select(
  col("sourceFile.name"),
  col("sourceFile.fetchedAt"),
  col("sourceFile.fileSize"),
  col("logMetadata")
).show(20, truncate = false)
```

### Streaming Mode

```scala
val stream = spark.readStream
  .format("rest-file")
  .option("remoteClient", "imperva")
  .option("uri", "https://my.imperva.com/api/audit-trail/v2/accounts/12345")
  .option("apiKey",    sys.env("IMPERVA_API_KEY"))
  .option("apiSecret", sys.env("IMPERVA_API_SECRET"))
  .option("enableSsl", "true")
  .option("pollInterval", "30s")          // poll every 30 seconds
  .option("maxFilesPerTrigger", "20")     // process up to 20 files per trigger
  .option("startOffset", "latest")        // skip historical files
  .option("asyncDownloads", "true")       // enable async prefetch
  .option("asyncPrefetchSize", "10")
  .load()

// Write to Delta Lake
stream
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/checkpoints/imperva-waf-logs")
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("60 seconds"))
  .start("/data/imperva/waf-logs")
```

#### Processing log content downstream

```scala
// Parse metadata JSON
import org.apache.spark.sql.functions._

val withParsedMeta = df
  .withColumn("meta", from_json(col("logMetadata"), schema = /* your metadata schema */))

// Filter only successfully downloaded files
val successful = df.filter(col("error").isNull)

// Detect failed downloads
val failed = df.filter(col("error").isNotNull)
failed.select("sourceFile.name", "error").show(truncate = false)
```

---

## Authentication

The client uses **HTTP Basic Authentication**:

```
Authorization: Basic base64(<apiKey>:<apiSecret>)
```

If `apiKey` and `apiSecret` are not provided, the client falls back to placeholder values (`mock-api-key` / `mock-api-secret`), which work against the local mock server during development.

**Best practice:** Never hardcode credentials. Use environment variables or a secrets manager:

```scala
.option("apiKey",    sys.env("IMPERVA_API_KEY"))
.option("apiSecret", sys.env("IMPERVA_API_SECRET"))
```

---

## SSL / TLS

For production use, always enable SSL:

```scala
.option("enableSsl", "true")
```

If your environment requires a custom CA certificate (e.g. corporate proxy):

```scala
.option("enableSsl",          "true")
.option("trustStorePath",     "/etc/ssl/certs/corporate-ca.jks")
.option("trustStorePassword", sys.env("TRUST_STORE_PASSWORD"))
```

---

## Retry Logic

The client wraps its HTTP backend with `RetryingBackendWrapper`, which automatically retries on:

- Network-level errors (connection refused, timeout)
- HTTP 5xx responses
- HTTP 429 (Too Many Requests)

Retry behaviour is controlled by:

| Option | Default |
|---|---|
| `maxRetries` | `3` |
| `retryDelay` | `1s` |

---

## Integration Testing with Mock Server

A Python mock server is included for local development and CI testing. It simulates the Imperva Log Integration API without requiring real credentials.

### Starting the mock server

```powershell
# Windows (PowerShell)
.\start-mock-server.ps1
```

```bash
# macOS / Linux
python3 mock-server/server.py
```

The server starts on `http://localhost:5000`.

### Available scenarios

| Scenario | Description |
|---|---|
| `baseline` | 5 static log files, no failures |

The scenario is embedded in the URI path:

```
http://localhost:5000/scenario/baseline/<accountId>
```

### Session isolation

For parallel test execution, pass a unique `sessionId` to isolate scenario state between test runs:

```scala
.option("sessionId", java.util.UUID.randomUUID().toString)
```

### Running integration tests

```bash
# Start the mock server first, then:
mvn test -P integration-tests -Dtest=MockServerIntegrationTest

# Streaming integration test
mvn test -P integration-tests -Dtest=StreamingIntegrationTest

# Imperva client unit tests (no server required)
mvn test -Dtest=IncapsulaClientTest
```

---

## Development Notes

### Project structure

```
src/main/scala/
  com/amadeus/spark/datasource/   ← Generic rest-file framework (open-source)
    remote/
      client/
        ImpervaLogParser.scala    ← Parses |==| binary format
        RemoteFileClient.scala    ← Client interface
        ClientRegistry.scala      ← ServiceLoader-based discovery
      conf/
        RemoteFileDataSourceOptions.scala
      streaming/
        RemoteMicroBatchStream.scala
      read/
        RemoteFileBatch.scala
  com/imperva/spark/datasource/   ← Imperva-specific implementation
    client/
      IncapsulaClient.scala       ← Main Imperva client
    config/
      DurationParser.scala
    util/
      RetryingBackendWrapper.scala

src/main/resources/META-INF/services/
  com.amadeus.spark.datasource.remote.client.RemoteFileClientRegister
  ← registers IncapsulaClient under short name "imperva"
```

### Adding support for a new Imperva API version

1. Update `listLogFiles()` in `IncapsulaClient` to call the new index endpoint.
2. Update `downloadLogFileWithResult()` for the new file download URL pattern.
3. If the log file binary format changes, update `ImpervaLogParser`.
4. Add/update integration tests in `IncapsulaClientIntegrationTest`.

### Dependency versions

| Library | Version |
|---|---|
| Apache Spark | 3.5.0 |
| Scala | 2.12.18 |
| sttp client4 | 4.0.13 |
| json4s | (transitive via sttp) |
| ScalaTest | 3.2.17 |

