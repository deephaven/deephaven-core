//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static io.deephaven.engine.table.Table.BARRAGE_COMPRESSION_ATTRIBUTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE} must stay on the table it was set on and not leak into derived tables.
 */
public class BarrageCompressionAttributeTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void attributeIsNotCopiedToDerivedTables() {
        final Table source = TableTools.emptyTable(10).update("A = ii", "B = ii % 3");
        final Table compressed = source.withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd,gzip"));
        assertEquals("zstd,gzip", compressed.getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));

        assertNull(compressed.sort("B").getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertNull(compressed.where("A > 2").getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertNull(compressed.update("C = A * 2").getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertNull(compressed.updateView("C = A * 2").getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        // flatten() of an already-flat table returns the same table, so flatten a filtered one
        final Table notFlat = source.where("A > 2").withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd"));
        assertNull(notFlat.flatten().getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertNull(compressed.naturalJoin(source, "A", "B2 = B").getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertNull(compressed.lastBy("B").getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void attributeSurvivesAttributeChangesAndCoalescing() {
        final Table compressed = TableTools.emptyTable(10).update("A = ii")
                .withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "snappy"));
        assertEquals("snappy",
                compressed.withAttributes(Map.of("Other", "value")).getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertEquals("snappy", compressed.coalesce().getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));

        // an uncoalesced source table's attributes are carried through coalesce()
        assertTrue(BaseTable.shouldCopyAttribute(BARRAGE_COMPRESSION_ATTRIBUTE,
                BaseTable.CopyAttributeOperation.Coalesce));
        for (final BaseTable.CopyAttributeOperation op : BaseTable.CopyAttributeOperation.values()) {
            if (op != BaseTable.CopyAttributeOperation.Coalesce) {
                assertFalse(op.name(), BaseTable.shouldCopyAttribute(BARRAGE_COMPRESSION_ATTRIBUTE, op));
            }
        }
    }
}
