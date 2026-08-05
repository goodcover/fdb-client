# Repository Guidelines

## Project Structure & Module Organization

This is a Mill-built Scala library, cross-compiled for Scala 2.13 and 3. Core FoundationDB bindings live in `fdb-core/`; record-layer support is in `fdb-record/`, event sourcing in `fdb-record-es/`, Pekko integration in `fdb-record-es-pekko/`, Lucene support in `fdb-record-lucene/`, and Spark integration in `fdb-spark/`. Companion `*-tests/` modules hold shared fixtures and suites. Follow the standard layout: production code under `src/main/scala`, tests under `src/test/scala`, configuration under `src/main/resources`, and schemas under `src/main/protobuf`. Build definitions are centralized in `build.mill`.

## Build, Test, and Development Commands

- `nix develop` enters the pinned JDK, Mill, and FoundationDB development environment.
- `docker compose up -d` starts the local FoundationDB 7.3 service required by database-backed tests.
- `./mill __[2.13.18].test` runs all non-Spark tests for Scala 2.13; substitute `3.3.8` for Scala 3.
- `./mill __[2.13.18,3.5.8].test` runs the Spark 3.5 test matrix. CI also covers Spark 4.1.0.
- `./mill __.compile` compiles all resolved modules.
- `./mill __.reformat` applies Scalafmt; `./mill __.checkFormat` checks formatting without changing files.
- `./mill scoverage.xmlReportAll` creates the aggregate XML coverage report under `out/`.

## Coding Style & Naming Conventions

Use Scalafmt 3.11.3 and the repository `.scalafmt.conf`; the maximum line length is 130. Use two-space indentation, sorted imports, and trailing commas where they improve multiline diffs. Follow Scala conventions: `PascalCase` for types and objects, `camelCase` for values and methods, and lowercase package paths such as `com.goodcover.fdb.record`. Keep Java interoperability code in `src/main/java` and generated protobuf output out of version control.

## Testing Guidelines

Most modules use ZIO Test; Pekko and streaming suites use ScalaTest. Name suites `*Spec.scala` and place reusable layers or fixtures in the matching `*-tests/src/main/scala` tree. Add regression coverage for fixes and run both supported Scala versions when changing shared code. No fixed coverage threshold is enforced, but CI publishes scoverage results.

## Commit & Pull Request Guidelines

Use short, imperative, sentence-case subjects, matching history (for example, `Avoid mutating snapshot query cursors during deletion`). Keep dependency or formatting-only changes separate from behavior changes. Pull requests should explain the problem and solution, identify affected modules, link relevant issues, and report the exact tests run. Ensure the full Scala/Spark matrix and formatting checks pass before requesting review.
