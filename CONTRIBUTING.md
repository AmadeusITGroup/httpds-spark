# Contributing to httpds-spark

Thank you for your interest in contributing to **httpds-spark**! This guide will help you get started.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Features](#suggesting-features)
  - [Submitting Pull Requests](#submitting-pull-requests)
- [Development Workflow](#development-workflow)
  - [Branching Strategy](#branching-strategy)
  - [Commit Messages](#commit-messages)
  - [Building](#building)
  - [Running Tests](#running-tests)
- [Coding Guidelines](#coding-guidelines)
  - [Style & Formatting](#style--formatting)
  - [Auto-formatting Setup](#auto-formatting-setup)
  - [Code Quality](#code-quality)
  - [Documentation](#documentation)
- [Pull Request Review Process](#pull-request-review-process)
- [License](#license)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

### Prerequisites

| Requirement  | Version |
|--------------|---------|
| Java (JDK)   | 11+     |
| Scala        | 2.12    |
| sbt          | 1.12+   |
| Apache Spark | 3.5.x   |
| Git          | 2.x+    |

### Development Setup

1. **Fork** the repository on GitHub.

2. **Clone** your fork locally:

   ```bash
   git clone https://github.com/<your-username>/httpds-spark.git
   cd httpds-spark
   ```

3. **Compile** the project to verify your setup:

   ```bash
   sbt compile
   ```

4. **Run the tests** to make sure everything passes:

   ```bash
   sbt test
   ```

   Tests use [ScalaTest](https://www.scalatest.org/) and [WireMock](https://wiremock.org/) — no external services are required.

## How to Contribute

### Reporting Bugs

Before opening a bug report, please search existing issues to avoid duplicates. When filing a new issue, include:

- A clear, descriptive title.
- Steps to reproduce the problem.
- Expected vs. actual behavior.
- Spark, Scala, and Java versions.
- Relevant logs, stack traces, or error messages.

### Suggesting Features

Feature requests are welcome. Please open an issue and describe:

- The problem or use-case the feature would address.
- Your proposed solution or API design (if any).
- Whether you're willing to implement it yourself.

### Submitting Pull Requests

1. Create a feature branch from `main` (see [Branching Strategy](#branching-strategy)).
2. Make your changes following the [Coding Guidelines](#coding-guidelines).
3. Add or update tests to cover your changes.
4. Ensure the full test suite passes (`sbt test`).
5. Push to your fork and open a pull request against `main`.

## Development Workflow

### Branching Strategy

| Branch type | Naming convention              | Example                        |
|-------------|--------------------------------|--------------------------------|
| Feature     | `feature/<short-description>`  | `feature/add-retry-backoff`    |
| Bug fix     | `fix/<short-description>`      | `fix/null-pointer-on-empty`    |
| Docs        | `docs/<short-description>`     | `docs/update-readme`           |

Always branch from the latest `main`.

### Commit Messages

Write clear, concise commit messages. We recommend the [Conventional Commits](https://www.conventionalcommits.org/) style:

```
<type>(<scope>): <short summary>

<optional body>
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`.

Examples:

```
feat(client): add exponential backoff to retry logic
fix(streaming): correct offset tracking when startOffset is "latest"
docs: update CONTRIBUTING guide
```

### Building

```bash
sbt compile    # Compile sources
sbt package    # Build the JAR (output in target/scala-2.12/)
```

### Running Tests

```bash
sbt test                             # Run all tests
sbt "testOnly *ClientRegistryTest"   # Run a specific test class
```

> **Note:** Tests are forked (`Test / fork := true`) with JVM options for Java 17+ compatibility. This is already configured in `build.sbt`.

## Coding Guidelines

### Style & Formatting

- Follow the official [Scala Style Guide](https://docs.scala-lang.org/style/).
- The project uses [Scalafmt](https://scalameta.org/scalafmt/) for automatic formatting (config: `.scalafmt.conf`).
- Max line length: **180 characters**.
- Docstring style: **JavaDoc**.
- **All code must be formatted before submitting a PR.** Run formatting manually if needed:

  ```bash
  sbt scalafmtAll        # Format main + test sources
  sbt scalafmtCheck      # Verify formatting (CI-friendly)
  ```

### Auto-formatting Setup

#### IntelliJ IDEA

1. Go to **Settings → Editor → Code Style → Scala**.
2. On the **Scalafmt** tab, select **scalafmt** as the formatter.
3. Enable **Reformat on file save**.
4. Click **Apply**.

#### VS Code

Install the [Metals](https://scalameta.org/metals/) extension — it picks up `.scalafmt.conf` automatically and formats on save.

### Code Quality

The project uses [Scalafix](https://scalacenter.github.io/scalafix/) to enforce code quality rules beyond formatting.

**Enabled rules (configured in `.scalafix.conf`):**

| Rule                           | Purpose                                  |
|--------------------------------|------------------------------------------|
| `OrganizeImports`              | Sorts and deduplicates imports           |
| `RedundantSyntax`              | Removes unnecessary syntax               |
| `DisableSyntax.noVars`         | Discourages mutable `var` state          |
| `DisableSyntax.noNulls`        | Discourages `null` literals              |
| `DisableSyntax.noAsInstanceOf` | Discourages unchecked casts              |
| `DisableSyntax.noIsInstanceOf` | Discourages runtime type checks          |
| `DisableSyntax.noReturns`      | Discourages non-tail `return` statements |
| `DisableSyntax.noXml`          | Discourages XML literals                 |
| `LeakingImplicitClassVal`      | Prevents leaking implicit class vals     |
| `NoValInForComprehension`      | Removes redundant `val` inside `for`     |

**Running Scalafix locally:**

```bash
sbt "scalafixAll --check"   # Check for violations (CI-friendly, no changes)
sbt scalafixAll              # Auto-fix what can be fixed automatically
```

**Convenience aliases:**

```bash
sbt lint      # scalafmtCheck + scalafixAll --check (full read-only lint)
sbt lintFix   # scalafmtAll + scalafixAll            (auto-fix everything)
```

**Suppressing a rule for legacy or interop code:**

If a violation cannot be removed (e.g., `null` required by a Java API), suppress it with a justification comment:

```scala
// Spark MicroBatchStream Java API requires returning null when no new data is available
// scalafix:off DisableSyntax.null
null
// scalafix:on DisableSyntax.null
```

Every `// scalafix:off` **must** be accompanied by a comment explaining why the suppression is necessary.

### Documentation

- **Public API methods must be documented** with ScalaDoc.
- Use `@param`, `@return`, and `@throws` tags where appropriate.
- Update the `README.md` if your change affects user-facing behavior or configuration.

## Pull Request Review Process

1. All PRs require at least **one approving review** before merging.
2. Maintainers may request changes — please address feedback promptly.
3. Keep PRs focused: one feature or fix per PR makes review faster.
4. Ensure the PR description includes:
   - **What** the change does and **why**.
   - A link to the related issue (if any).
   - Any breaking changes or migration steps.
5. Once approved, the integration and release of the new version is automatically handled via CI.

## License

By contributing to httpds-spark, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
