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

import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.table.Table.BARRAGE_COMPRESSION_ATTRIBUTE;
import static io.deephaven.extensions.barrage.BarrageCompressionAttribute.withCompression;
import static io.deephaven.grpc.compression.CompressionType.GZIP;
import static io.deephaven.grpc.compression.CompressionType.SNAPPY;
import static io.deephaven.grpc.compression.CompressionType.ZSTD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class BarrageCompressionAttributeTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void setsTheAttributeInPreferenceOrder() {
        final Table source = TableTools.emptyTable(10);

        assertEquals("snappy,zstd,gzip",
                withCompression(source, SNAPPY, ZSTD, GZIP).getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertEquals("gzip,zstd",
                withCompression(source, List.of(GZIP, ZSTD)).getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertEquals("zstd,gzip",
                withCompression(source, ZSTD, GZIP, ZSTD).getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertNull(source.getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void noTypesSetsAnEmptyList() {
        final Table compressed = withCompression(TableTools.emptyTable(10), ZSTD);

        // an empty list, rather than no attribute, so that the server's default compression types do not apply
        assertEquals("", withCompression(compressed).getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertEquals("", withCompression(compressed, List.of()).getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertEquals("zstd", compressed.getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void nullTypesAreRejected() {
        final Table source = TableTools.emptyTable(10);
        assertThrows(NullPointerException.class, () -> withCompression(source, ZSTD, null));
        assertThrows(NullPointerException.class, () -> withCompression(source, Arrays.asList(null, GZIP)));
    }

    @Test
    public void rollupAndTreeTables() {
        final Table source = TableTools.emptyTable(10).update("ID = ii", "Parent = ii == 0 ? NULL_LONG : 0L", "B = ii");

        final RollupTable rollup = withCompression(source.rollup(List.of(Aggregation.AggSum("B")), "ID"), ZSTD);
        assertEquals("zstd", rollup.getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));

        final TreeTable tree = withCompression(source.tree("ID", "Parent"), SNAPPY, GZIP);
        assertEquals("snappy,gzip", tree.getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
    }
}
