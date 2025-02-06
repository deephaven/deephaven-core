# Overview

Code coverage for Deephaven Community Core manages multiple languages like Java, Python, R, Go and C++. This is handled in the gradle build at the individual project level but also supports collection of normalized results rolled up to the top level. For convenience, both top-level Java HTML and a top-level all-language CSV are created.

## Running for Coverage

A typical run looks like the following that is run from the root of the multi-project build
```
./gradlew -Pcoverage.enabled=true check
./gradlew -Pcoverage.enabled=true testSerial
./gradlew -Pcoverage.enabled=true testParallel
./gradlew -Pcoverage.enabled=true testOutOfBand
./gradlew -Pcoverage.enabled=true jacocoTestReport
./gradlew -Pcoverage.enabled=true coverage-merge
```
Running the second command is not contingent upon the first command succeeding. It merely collects what coverage is available.

## Result Files

Results for individual project coverage are stored in the project's _build_ output directory. Depending on the language and coverage tools, there will be different result files with slightly different locations and names. For example, Java coverage could produce a binary _jacoco.exec_ file, while python coverage produces a tabbed text file.

Aggregated results produce a merged CSV file for each language under the top-level _build_ directory. Those CSV files are further merged into one _all-coverage.csv_.

## Exclusion Filters

In some cases, there may be a need to exclude some packages from coverage, even though they may be used during testing. For example, some Java classes used in GRPC are generated. The expectation is that the generator mechanism has already been tested and should produce viable classes. Including coverage for those classes in the results as zero coverage causes unnecessary noise and makes it harder to track coverage overall.

To avoid unneeded coverage, the file _exclude-packages.txt_ can be used. This is a list of values to be excluded if they match the "Package" column in the coverage CSV. These are exact values and not wildcards.

## File Layout

Top-level Build Directory (Some languages TBD)
- `coverage/` This project's directory
  - `gather-coverage.py` Gather and normalize coverage for all languages
  - `exclude-packages.txt` A list of packages to exclude from aggregated results
- `buildSrc/src/main/groovy/`
  - `io.deephaven.java-jacoco-conventions.gradle` Applied to run coverage on Java projects
  - `io.deephaven.java-test-conventions.gradle` Applies the above conditionally base on the _coverage.enabled_ property
- `coverage/build/reports/coverage/`
  - `java-coverage.csv` Normalized coverage from all Java projects
  - `python-coverage.py` Normalized coverage from all Python projects
  - `cplus-coverage.py` Normalized coverage from all C++ projects
  - `r-coverage.py` Normalized coverage from all R projects
  - `go-coverage.oy` Normalized coverage from all Go projects
  - `all-coverage.csv` Normalized and filtered coverage from all covered projects
- `coverage/build/reports/jacoco/jacoco-merge/html/`
  - `index.html` Root file to view Java coverage down to the branch level (not filtered)
