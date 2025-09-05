//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test each of the {@link RowGroupInfo} configurations.
 */
@SuppressWarnings("SameParameterValue")
public class TestRowGroupInfo {

    /**
     * A helper method to verify that ... we are able to detect inequality between {@link RowGroupInfo} instances
     *
     * @param l a {@link RowGroupInfo} instance, which is not equal to {@code r}
     * @param r a {@link RowGroupInfo} instance, which is not equal to {@code l}
     */
    private void assertInequality(final @NotNull RowGroupInfo l, final @NotNull RowGroupInfo r) {
        assertNotEquals(String.format("%s != %s", l, r), l, r);
        assertNotEquals(String.format("%s != %s", r, l), r, l);
    }

    /**
     * Verify that equalities between {@link RowGroupInfo} instances
     */
    @Test
    public void testEqualities() {
        final List<RowGroupInfo> distinctConfigs = List.of(
                RowGroupInfo.singleRowGroup(),
                RowGroupInfo.withMaxRows(42),
                RowGroupInfo.withMaxRows(41),
                RowGroupInfo.splitEvenly(13),
                RowGroupInfo.splitEvenly(1),
                RowGroupInfo.byGroup("a", "b"),
                RowGroupInfo.byGroup(99, "a", "b"));
        final int L = distinctConfigs.size();
        for (int i = 0; i < L - 1; ++i) {
            final RowGroupInfo config = distinctConfigs.get(i);
            assertEquals(String.format("%s == %s", config, config), config, config);
            for (int j = i + 1; j < L; ++j) {
                assertInequality(config, distinctConfigs.get(j));
            }
        }
    }
}
