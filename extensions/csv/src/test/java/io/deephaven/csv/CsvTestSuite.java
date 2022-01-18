package io.deephaven.csv;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({CsvReaderTest.class,
        TestCsvTools.class,
        DeephavenCsvTest.class})
public class CsvTestSuite {
}
