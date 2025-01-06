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
        read("London_v1_Whitespace_SNAPPY");
    }

    @Test
    public void Product_v1() {
        read("Product_v1_CPP_SNAPPY_Whitespace");
    }

    @Test
    public void alltypes() {
        read("alltypes_dictionary_v1_IMPALA_NONE");
        read("alltypes_plain_v1_IMPALA_NONE");
        read("alltypes_plain_v1_IMPALA_SNAPPY");
    }

    @Test
    public void customer() {
        read("customer_v1_IMPALA_GZIP");
        read("customer_v1_IMPALA_NONE");
        read("customer_v1_IMPALA_SNAPPY");
    }

    @Test
    public void eth() {
        final String prefix = "eth_";
        for (String part : new String[] {"cBROTLI_L9", "cBROTLI_PS5"}) {
            codecFail(prefix + part);
        }
        for (String part : new String[] {"cGZIP_PS5", "cLZ4_PS5", "cNONE_PS5", "cSNAPPY_PS5", "cZSTD_L9",
                "cZSTD_PS5"}) {
            read(prefix + part);
        }
    }

    @Test
    public void eth_r2_v1() {
        final String prefix = "eth_r2_v1_";
        for (String part : new String[] {"cBROTLI"}) {
            codecFail(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cGZIP_l5", "cLZ4", "cNONE", "cSNAPPY", "cZSTD", "cZSTD_l20"}) {
            read(prefix + part);
        }
    }

    @Test
    public void eth_r2_v1_p2() {
        read("eth_r2_v1_p2");
    }

    @Test
    public void eth_r2_v2() {
        final String prefix = "eth_r2_v2_";
        for (String part : new String[] {"cBROTLI"}) {
            codecFail(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            read(prefix + part);
        }
    }

    @Test
    public void eth_v1() {
        final String prefix = "eth_v1_";
        for (String part : new String[] {"cBROTLI", "cBROTLI_l10", "cBROTLI_l100"}) {
            codecFail(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cGZIP_l5", "cGZIP_l9", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            read(prefix + part);
        }
    }

    @Test
    public void eth_v1_p1() {
        final String prefix = "eth_v1_p1_";
        for (String part : new String[] {"cBROTLI"}) {
            codecFail(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNSAPPY", "cZSTD"}) {
            read(prefix + part);
        }
    }

    @Test
    public void eth_v1_p2() {
        final String prefix = "eth_v1_p2_";
        for (String part : new String[] {"cBROTLI"}) {
            codecFail(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            read(prefix + part);
        }
    }

    @Test
    public void eth_v2() {
        final String prefix = "eth_v2_";
        for (String part : new String[] {"cBROTLI"}) {
            codecFail(prefix + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cLZ4_PS5", "cNONE", "cSNAPPY", "cZSTD", "fSpark"}) {
            read(prefix + part);
        }
    }

    @Test
    public void eth_v2_p1() {
        for (String part : new String[] {"cBROTLI"}) {
            codecFail("eth_v2_p1_" + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            read("eth_v2_p1_" + part);
        }
    }

    @Test
    public void eth_v2_p2() {
        for (String part : new String[] {"cBROTLI"}) {
            codecFail("eth_v2_p2_" + part);
        }
        for (String part : new String[] {"cGZIP", "cLZ4", "cNONE", "cSNAPPY", "cZSTD"}) {
            read("eth_v2_p2_" + part);
        }
    }

    @Test
    public void flow_v1() {
        read("flow_v1_NULLS_SNAPPY");
    }

    @Test
    public void nation() {
        read("nation_dict_MALFORMED");
    }

    @Test
    public void nested_maps_v1() {
        openFail("nested_maps_v1_NULLS_SNAPPY", UnsupportedOperationException.class,
                "Unsupported maximum repetition level");
    }

    @Test
    public void nested_v1() {
        openFail("nested_v1_NULLS_SNAPPY", UnsupportedOperationException.class, "Unsupported maximum repetition level");
    }

    @Test
    public void nonnullable_nested_v1() {
        openFail("nonnullable_nested_v1_IMPALA_NULLS_NONE", UnsupportedOperationException.class,
                "Unsupported maximum repetition level");
    }

    @Test
    public void nulls_v1() {
        // Note: it would be better if we could figure this out before actually reading a column.
        readFail("nulls_v1_NULLS_SNAPPY", TableInitializationException.class,
                "Failed to read parquet page because nested optional levels are not supported");
    }

    @Test
    public void repeated_nested() {
        openFail("repeated_nested_RUST_NONE", UnsupportedOperationException.class,
                "Encountered unsupported multi-column field");
    }

    @Test
    public void stock_v1() {
        read("stock_v1_CPP_SNAPPY");
    }

    @Test
    public void taxi() {
        read("taxi");
    }

    private static Table open(String name) {
        final String path = CHECKOUT_ROOT
                .resolve("parquetFiles")
                .resolve(name + ".parquet")
                .toUri()
                .toString();
        return ParquetTools.readTable(path);
    }

    private static void read(String name) {
        open(name).select();
    }

    private static void openFail(String name, Class<? extends Exception> error, String messagePart) {
        try {
            open(name);
            failBecauseExceptionWasNotThrown(error);
        } catch (Throwable e) {
            assertThat(e).isInstanceOf(error);
            assertThat(e).hasMessageContaining(messagePart);
        }
    }

    private static void readFail(String name, Class<? extends Exception> error, String messagePart) {
        try {
            read(name);
            failBecauseExceptionWasNotThrown(error);
        } catch (Throwable e) {
            assertThat(e).isInstanceOf(error);
            assertThat(e).getRootCause().hasMessageContaining(messagePart);
        }
    }

    private static void codecFail(String name) {
        // Note: it would be better if we could figure this out before actually reading a column.
        readFail(name, TableDataException.class, "Failed to find CompressionCodec");
    }
}
