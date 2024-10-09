//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.exceptions.TableInitializationException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.junit.Assume.assumeTrue;

/**
 * Assumes that there is already a checkout of
 * <a href="https://github.com/deephaven/deephaven-core-test-data">deephaven-core-test-data</a>. This is currently meant
 * to be run locally.
 */
public class ParquetToolsTestDataTest {

    private static final Path CHECKOUT_ROOT = null; // Path.of("/path/to/deephaven-core-test-data");

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @BeforeClass
    public static void beforeClass() {
        assumeTrue(CHECKOUT_ROOT != null);
    }

    @Test
    public void London_v1() {
        readData("London_v1_Whitespace_SNAPPY");
    }

    @Test
    public void Product_v1() {
        readData("Product_v1_CPP_SNAPPY_Whitespace");
    }

    @Test
    public void alltypes() {
        readData("alltypes_dictionary_v1_IMPALA_NONE");
        readData("alltypes_plain_v1_IMPALA_NONE");
        readData("alltypes_plain_v1_IMPALA_SNAPPY");
    }

    @Test
    public void customer() {
        readData("customer_v1_IMPALA_GZIP");
        readData("customer_v1_IMPALA_NONE");
        readData("customer_v1_IMPALA_SNAPPY");
    }

    @Test
    public void eth() {
        final String prefix = "eth_";
        for (String part : new String[] {"cBROTLI_L9", "cBROTLI_PS5"}) {
            noCodec(prefix + part);
        }
        for (String part : new String[] {"cGZIP_PS5", "cLZ4_PS5", "cNONE_PS5", "cSNAPPY_PS5", "cZSTD_L9",
                "cZSTD_PS5"}) {
            readData(prefix + part);
        }
    }

    @Test
    public void eth_r2_v1() {
        final String prefix = "eth_r2_v1_";
        for (String part : new String[] {"cBROTLI"}) {
            noCodec(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cGZIP_l5", "cLZ4", "cNONE", "cSNAPPY", "cZSTD", "cZSTD_l20"}) {
            readData(prefix + part);
        }
    }

    @Test
    public void eth_r2_v1_p2() {
        readData("eth_r2_v1_p2");
    }

    @Test
    public void eth_r2_v2() {
        final String prefix = "eth_r2_v2_";
        for (String part : new String[] {"cBROTLI"}) {
            noCodec(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            readData(prefix + part);
        }
    }

    @Test
    public void eth_v1() {
        final String prefix = "eth_v1_";
        for (String part : new String[] {"cBROTLI", "cBROTLI_l10", "cBROTLI_l100"}) {
            noCodec(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cGZIP_l5", "cGZIP_l9", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            readData(prefix + part);
        }
    }

    @Test
    public void eth_v1_p1() {
        final String prefix = "eth_v1_p1_";
        for (String part : new String[] {"cBROTLI"}) {
            noCodec(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNSAPPY", "cZSTD"}) {
            readData(prefix + part);
        }
    }

    @Test
    public void eth_v1_p2() {
        final String prefix = "eth_v1_p2_";
        for (String part : new String[] {"cBROTLI"}) {
            noCodec(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            readData(prefix + part);
        }
    }

    @Test
    public void eth_v2() {
        final String prefix = "eth_v2_";
        for (String part : new String[] {"cBROTLI"}) {
            noCodec(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cLZ4_PS5", "cNONE", "cSNAPPY", "cZSTD", "fSpark"}) {
            readData(prefix + part);
        }
    }

    @Test
    public void eth_v2_p1() {
        for (String part : new String[] {"cBROTLI"}) {
            noCodec("eth_v2_p1_" + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            readData("eth_v2_p1_" + part);
        }
    }

    @Test
    public void eth_v2_p2() {
        for (String part : new String[] {"cBROTLI"}) {
            noCodec("eth_v2_p2_" + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            readData("eth_v2_p2_" + part);
        }
    }

    @Test
    public void flow_v1() {
        readData("flow_v1_NULLS_SNAPPY");
    }

    @Test
    public void nation() {
        readData("nation_dict_MALFORMED");
    }

    @Test
    public void nested_maps_v1() {
        try {
            open("nested_maps_v1_NULLS_SNAPPY");
            failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            assertThat(e).hasMessageContaining("Unsupported maximum repetition level 2");
        }
    }

    @Test
    public void nested_v1() {
        try {
            open("nested_v1_NULLS_SNAPPY");
            failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            assertThat(e).hasMessageContaining("Unsupported maximum repetition level 3");
        }
    }

    @Test
    public void nonnullable_nested_v1() {
        try {
            open("nonnullable_nested_v1_IMPALA_NULLS_NONE");
            failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            assertThat(e).hasMessageContaining("Unsupported maximum repetition level 2");
        }
    }

    @Test
    public void nulls_v1() {
        try {
            // Note: it would be better if we could figure this out before actually reading a column.
            readData("nulls_v1_NULLS_SNAPPY");
            failBecauseExceptionWasNotThrown(TableInitializationException.class);
        } catch (TableInitializationException e) {
            assertThat(e).getRootCause().hasMessageContaining(
                    "Failed to read parquet page because nested optional levels are not supported");
        }
    }

    @Test
    public void repeated_nested() {
        try {
            open("repeated_nested_RUST_NONE");
            failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            assertThat(e).hasMessageContaining("Encountered unsupported multi-column field");
        }
    }

    @Test
    public void stock_v1() {
        readData("stock_v1_CPP_SNAPPY");
    }

    @Test
    public void taxi() {
        readData("taxi");
    }

    private static Table open(String name) {
        final String path = CHECKOUT_ROOT
                .resolve("parquetFiles")
                .resolve(name + ".parquet")
                .toUri()
                .toString();
        return ParquetTools.readTable(path);
    }

    private static void readData(String name) {
        open(name).select();
    }

    private static void noCodec(String name) {
        try {
            // Note: it would be better if we could figure this out before actually reading a column.
            readData(name);
        } catch (TableDataException e) {
            assertThat(e).getCause().hasMessageContaining("Failed to find CompressionCodec");
        }
    }
}
