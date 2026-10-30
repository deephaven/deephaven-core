//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.table.Table.TRANSPORT_COMPRESSION_ATTRIBUTE;
import static io.deephaven.extensions.barrage.TransportCompressionAttribute.withCompression;
import static io.deephaven.extensions.barrage.TransportCompressionAttribute.resetCompression;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TransportCompressionAttributeTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void setsTheAttributeInPreferenceOrder() {
        final Table source = TableTools.emptyTable(10);

        assertEquals("snappy,zstd,gzip",
                withCompression(source, "snappy", "zstd", "gzip").getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
        assertEquals("gzip,zstd",
                withCompression(source, List.of("gzip", "zstd")).getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
        assertEquals("zstd,gzip",
                withCompression(source, "zstd", "gzip", "zstd").getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
        assertNull(source.getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void namesAreNormalized() {
        assertEquals("zstd,gzip", withCompression(TableTools.emptyTable(10), " ZSTD ", "Gzip", "zstd")
                .getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void noTypesSetsAnEmptyList() {
        final Table compressed = withCompression(TableTools.emptyTable(10), "zstd");

        // an empty list, rather than no attribute, so that the server's default compression types do not apply
        assertEquals("", withCompression(compressed).getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
        assertEquals("", withCompression(compressed, List.of()).getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
        assertEquals("zstd", compressed.getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void unsupportedNamesAreRejected() {
        final Table source = TableTools.emptyTable(10);
        final IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> withCompression(source, "zstd", "zsdt"));
        assertEquals("Unsupported compression 'zsdt'; supported values are deflate, gzip, snappy, zstd",
                e.getMessage());
        // one name cannot smuggle in a list
        assertThrows(IllegalArgumentException.class, () -> withCompression(source, "zstd,gzip"));
        assertThrows(IllegalArgumentException.class, () -> withCompression(source, ""));
        assertThrows(IllegalArgumentException.class, () -> withCompression(source, "identity"));
    }

    @Test
    public void nullNamesAreRejected() {
        final Table source = TableTools.emptyTable(10);
        // null is neither "never compress" nor "the server's default", so each message names both
        for (final ThrowingRunnable call : List.<ThrowingRunnable>of(
                () -> withCompression(source, "zstd", null),
                () -> withCompression(source, Arrays.asList(null, "gzip")),
                () -> withCompression(source, (String[]) null),
                () -> withCompression(source, (List<String>) null))) {
            final NullPointerException e = assertThrows(NullPointerException.class, call);
            assertTrue(e.getMessage(), e.getMessage().contains("pass none to never compress"));
            assertTrue(e.getMessage(), e.getMessage().contains("resetCompression"));
        }
    }

    @Test
    public void resetCompressionRemovesTheAttribute() {
        final Table source = TableTools.emptyTable(10).update("ID = ii", "Parent = ii == 0 ? NULL_LONG : 0L", "B = ii");

        final Table compressed = withCompression(source, "zstd");
        assertNull(resetCompression(compressed).getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
        // also undoes an empty list, which never compresses
        assertNull(resetCompression(withCompression(source)).getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
        assertNull(resetCompression(source).getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
        assertEquals("zstd", compressed.getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));

        final RollupTable rollup = withCompression(source.rollup(List.of(Aggregation.AggSum("B")), "ID"), "zstd");
        assertNull(resetCompression(rollup).getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));

        final TreeTable tree = withCompression(source.tree("ID", "Parent"), "snappy");
        assertNull(resetCompression(tree).getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void rollupAndTreeTables() {
        final Table source = TableTools.emptyTable(10).update("ID = ii", "Parent = ii == 0 ? NULL_LONG : 0L", "B = ii");

        final RollupTable rollup = withCompression(source.rollup(List.of(Aggregation.AggSum("B")), "ID"), "zstd");
        assertEquals("zstd", rollup.getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));

        final TreeTable tree = withCompression(source.tree("ID", "Parent"), "snappy", "gzip");
        assertEquals("snappy,gzip", tree.getAttribute(TRANSPORT_COMPRESSION_ATTRIBUTE));
    }
}
