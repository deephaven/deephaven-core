//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTableInfo {
    @Test
    public void defaults() {
        final TableInfo ti = TableInfo.builder().build();

        assertThat(ti.version()).isEqualTo("unknown");
        assertTrue(ti.groupingColumns().isEmpty());
        assertTrue(ti.dataIndexes().isEmpty());
        assertTrue(ti.columnTypes().isEmpty());
        assertTrue(ti.sortingColumns().isEmpty());

        // Derived maps are also empty when lists are empty
        assertTrue(ti.groupingColumnMap().isEmpty());
        assertTrue(ti.columnTypeMap().isEmpty());
    }

    @Test
    public void version() {
        assertThat(TableInfo.builder()
                .version("1.2.3-test")
                .build()
                .version())
                .isEqualTo("1.2.3-test");
    }

    @Test
    public void sortingColumns() {
        final SortColumnInfo sci1 = SortColumnInfo.of("A", SortColumnInfo.SortDirection.Ascending);
        final SortColumnInfo sci2 = SortColumnInfo.of("B", SortColumnInfo.SortDirection.Descending);
        assertThat(TableInfo.builder()
                .addSortingColumns(sci1, sci2)
                .build()
                .sortingColumns())
                .isEqualTo(List.of(sci1, sci2));
    }

    @Test
    public void dataIndexes() {
        final DataIndexInfo di1 = DataIndexInfo.of("Idx1", "A");
        final DataIndexInfo di2 = DataIndexInfo.of("Idx2", "B");
        assertThat(TableInfo.builder()
                .addDataIndexes(di1, di2)
                .build()
                .dataIndexes())
                .isEqualTo(List.of(di1, di2));
    }

    @Test
    public void columnTypes() {
        final ColumnTypeInfo ct1 = ColumnTypeInfo.builder().columnName("Col1").build();
        final ColumnTypeInfo ct2 = ColumnTypeInfo.builder().columnName("Col2").build();
        assertThat(TableInfo.builder()
                .addColumnTypes(ct1, ct2)
                .build()
                .columnTypes())
                .isEqualTo(List.of(ct1, ct2));
    }

    @Test
    public void withColumnTypes() {
        final TableInfo original = TableInfo.builder().version("0.5.0").build();

        final ColumnTypeInfo cti = ColumnTypeInfo.builder().columnName("Col1").build();
        final TableInfo copy = original.withColumnTypes(cti);

        // original unchanged
        assertThat(original.columnTypes().size()).isEqualTo(0);

        // copy has a new column type
        assertThat(copy.version()).isEqualTo("0.5.0");
        assertThat(copy.columnTypes()).isEqualTo(List.of(cti));
    }

    @Test
    public void jsonRoundTrip() throws Exception {
        final TableInfo ti = TableInfo.builder()
                .version("0.3.5")
                .build();

        final String json = ti.serializeToJSON();
        assertThat(json).contains("\"version\":\"0.3.5\"");

        final TableInfo back = TableInfo.deserializeFromJSON(json);
        assertThat(back.version()).isEqualTo("0.3.5");
        assertTrue(back.groupingColumns().isEmpty());
        assertTrue(back.columnTypes().isEmpty());
        assertTrue(back.dataIndexes().isEmpty());
        assertTrue(back.sortingColumns().isEmpty());
    }
}
