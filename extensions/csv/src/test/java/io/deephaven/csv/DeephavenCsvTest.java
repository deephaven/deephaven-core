//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.csv;

import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.apache.commons.io.input.ReaderInputStream;
import org.junit.Rule;
import org.junit.Test;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;

public class DeephavenCsvTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void instantCustomTimezone() throws CsvReaderException {
        final ZoneId nycId = ZoneId.of("America/New_York");
        Instant INSTANT_A = LocalDateTime.of(2019, 5, 2, 19, 33, 12, 123456789).atZone(nycId).toInstant();
        Instant INSTANT_B = LocalDateTime.of(2017, 2, 2, 3, 18, 55, 987654321).atZone(nycId).toInstant();

        final String input = "" +
                "Timestamp\n" +
                "2019-05-02 19:33:12.123456789 NY\n" +
                "\n" +
                "2017-02-02T03:18:55.987654321 NY\n";

        final Table expected = TableTools.newTable(TableTools.col("Timestamp", INSTANT_A, null, INSTANT_B));

        invokeTest(input, CsvTools.builder().build(), expected);
    }

    private static void invokeTest(String input, CsvSpecs specs, Table expected) throws CsvReaderException {
        final StringReader reader = new StringReader(input);
        final ReaderInputStream inputStream = new ReaderInputStream(reader, StandardCharsets.UTF_8);
        final Table actual = CsvTools.readCsv(inputStream, specs);
        assertTableEquals(expected, actual);
    }
}
