//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.util.type.ArrayTypeUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class ModifiedColumnSetTest {
    @Test
    public void testModifiedColumnSet() {
        Map<String, ColumnSource<?>> columns = new HashMap<String, ColumnSource<?>>();
        String[] cols = new String[] {"Col1", "Col2", "Col3"};
        Arrays.stream(cols).forEach(col -> columns.put(col, null));

        ModifiedColumnSet mcs = new ModifiedColumnSet(columns);
        mcs.setAllDirty();
        String[] dirtyColumnNames = mcs.dirtyColumnNames();
        assertArrayEquals(dirtyColumnNames, cols);

        mcs.clear();
        dirtyColumnNames = mcs.dirtyColumnNames();
        assertArrayEquals(dirtyColumnNames, ArrayTypeUtils.EMPTY_STRING_ARRAY);

        mcs.setAll("Col2", "Col3");
        dirtyColumnNames = mcs.dirtyColumnNames();
        assertArrayEquals(dirtyColumnNames, new String[] {"Col2", "Col3"});

        BitSet bitSet = mcs.extractAsBitSet();
        BitSet expected_bitSet = new BitSet(3);
        expected_bitSet.set(1, 3);
        assertTrue(bitSet.equals(expected_bitSet));

        mcs.clearAll("Col2", "Col3");
        dirtyColumnNames = mcs.dirtyColumnNames();
        assertArrayEquals(dirtyColumnNames, ArrayTypeUtils.EMPTY_STRING_ARRAY);

    }
}
