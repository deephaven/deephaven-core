package io.deephaven.csv;

import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTime;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class DeephavenCsvTest {
    @Test
    public void dateTimeCustomTimezone() throws CsvReaderException {
        final ZoneId nycId = ZoneId.of("America/New_York");
        final DateTime DATETIME_A =
                DateTime.of(LocalDateTime.of(2019, 5, 2, 19, 33, 12, 123456789).atZone(nycId).toInstant());
        final DateTime DATETIME_B =
                DateTime.of(LocalDateTime.of(2017, 2, 2, 3, 18, 55, 987654321).atZone(nycId).toInstant());

        final String input = "" +
                "Timestamp\n" +
                "2019-05-02 19:33:12.123456789 NY\n" +
                "\n" +
                "2017-02-02T03:18:55.987654321 NY\n";

        final Table expected = TableTools.newTable(
                TableTools.col("Timestamp", DATETIME_A, null, DATETIME_B));

        invokeTest(input, CsvSpecs.csv(), expected);
    }

    private static void invokeTest(String input, CsvSpecs specs, Table expected) throws CsvReaderException {
        final Table actual = specs.parse(input);
        final String differences = TableTools.diff(actual, expected, 25);
        Assertions.assertThat(differences).isEmpty();
    }
}
