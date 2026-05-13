---
title: Local development with Deephaven libraries
sidebar_label: Local development
---

This guide explains how to build Java or Groovy projects that depend on Deephaven Community libraries. This is useful for creating utilities, custom functions, or extensions that work with Deephaven tables.

## Set up your project

Deephaven artifacts are published to Maven Central, so no credentials are required.

### Gradle

Add the Maven Central repository and Deephaven dependencies to your `build.gradle`:

```groovy skip-test
repositories {
    mavenCentral()
}

def dhcVersion = '0.37.0' // Use the version that matches your Deephaven server

dependencies {
    api "io.deephaven:deephaven-engine-api:$dhcVersion"
    api "io.deephaven:deephaven-engine-table:$dhcVersion"
    api "io.deephaven:deephaven-Configuration:$dhcVersion"
    api "io.deephaven:deephaven-FishUtil:$dhcVersion"
    implementation "io.deephaven:deephaven-log-factory:$dhcVersion"
}
```

### Maven

Add dependencies to your `pom.xml`:

```xml
<properties>
    <dhc.version>0.37.0</dhc.version>
</properties>

<dependencies>
    <dependency>
        <groupId>io.deephaven</groupId>
        <artifactId>deephaven-engine-api</artifactId>
        <version>${dhc.version}</version>
    </dependency>
    <dependency>
        <groupId>io.deephaven</groupId>
        <artifactId>deephaven-engine-table</artifactId>
        <version>${dhc.version}</version>
    </dependency>
</dependencies>
```

## Common dependencies by use case

The dependencies you need depend on what your project does. Here are common patterns:

| Use case                               | Dependencies                                     |
| -------------------------------------- | ------------------------------------------------ |
| Table operations (select, where, join) | `deephaven-engine-api`, `deephaven-engine-table` |
| Read/write CSV files                   | `deephaven-extensions-csv`                       |
| Read/write Parquet files               | `deephaven-extensions-parquet-table`             |
| Configuration utilities                | `deephaven-Configuration`                        |
| Date/time utilities                    | `deephaven-FishUtil`                             |
| Logging                                | `deephaven-log-factory`                          |

Browse all available modules on [Maven Central](https://mvnrepository.com/artifact/io.deephaven).

## Local unit testing

To use Deephaven table operations in unit tests, you need to open an [`ExecutionContext`](../../conceptual/execution-context.md).

### JVM configuration

Deephaven logs JVM internal stats that require the following JVM argument:

```
--add-exports=java.management/sun.management=ALL-UNNAMED
```

Add this to your Gradle test task:

```groovy skip-test
test {
    jvmArgs '--add-exports=java.management/sun.management=ALL-UNNAMED'
}
```

Or in Maven:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <argLine>--add-exports=java.management/sun.management=ALL-UNNAMED</argLine>
    </configuration>
</plugin>
```

### Example test setup

Use JUnit with an ExecutionContext to test table operations:

```java skip-test
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MyTableUtilsTest {

    private static ExecutionContext executionContext;

    @BeforeAll
    public static void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContext.open();
    }

    @AfterAll
    public static void tearDown() {
        executionContext.close();
    }

    @Test
    public void testTableOperation() {
        // Your test code here
    }
}
```

### Reading test data

Load test data from CSV or Parquet files in your test resources:

```java skip-test
import io.deephaven.csv.CsvTools;
import io.deephaven.parquet.table.ParquetTools;

// Read CSV from test resources
Table csvTable = CsvTools.readCsv(getClass().getResourceAsStream("/test-data.csv"));

// Read Parquet from test resources
Table parquetTable = ParquetTools.readTable(getClass().getResource("/test-data.parquet").getPath());
```

## Related documentation

- [Execution context](../conceptual/execution-context.md)
- [Extract table values](./extract-table-value.md)
- [Application mode](./application-mode.md)
