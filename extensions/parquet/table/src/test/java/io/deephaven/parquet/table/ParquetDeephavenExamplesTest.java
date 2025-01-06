//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Assumes that there is already a checkout of <a href="https://github.com/deephaven/examples">deephaven-examples</a>.
 * This is currently meant to be run locally.
 */
public class ParquetDeephavenExamplesTest {

    private static final Path CHECKOUT_ROOT = null; // Path.of("/path/to/deephaven-examples");

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @BeforeClass
    public static void beforeClass() {
        assumeTrue(CHECKOUT_ROOT != null);
    }

    @Test
    public void pems() {
        read("Pems/parquet/pems");
    }

    @Test
    public void crypto() {
        read("CryptoCurrencyHistory/Parquet/crypto.parquet");
        read("CryptoCurrencyHistory/Parquet/crypto_sept7.parquet");
        read("CryptoCurrencyHistory/Parquet/crypto_sept8.parquet");
        read("CryptoCurrencyHistory/Parquet/CryptoTrades_20210922.parquet");
        read("CryptoCurrencyHistory/Parquet/FakeCryptoTrades_20230209.parquet");
    }

    @Test
    public void taxi() {
        read("Taxi/parquet/taxi.parquet");
    }

    @Test
    public void sensorData() {
        read("SensorData/parquet/SensorData_gzip.parquet");
    }

    @Test
    public void grades() {
        assertTableEquals(
                read("ParquetExamples/grades"),
                read("ParquetExamples/grades_meta"));
        assertTableEquals(
                read("ParquetExamples/grades_flat").sort("Name", "Class"),
                read("ParquetExamples/grades_flat_meta").sort("Name", "Class"));
        assertTableEquals(
                read("ParquetExamples/grades_kv").sort("Name", "Class"),
                read("ParquetExamples/grades_kv_meta").sort("Name", "Class"));
    }

    private static Table read(String name) {
        final String path = CHECKOUT_ROOT
                .resolve(name)
                .toUri()
                .toString();
        return ParquetTools.readTable(path).select();
    }
}
