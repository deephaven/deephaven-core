package io.deephaven.qst.table;

import io.deephaven.qst.column.header.ColumnHeader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryAppendOnlyInputTableTest {
    @Test
    void headerNotEquals() {
        final TableHeader header = TableHeader.of(ColumnHeader.ofInt("Foo"));
        final TableSpec create1 = InMemoryAppendOnlyInputTable.of(header);
        final TableSpec create2 = InMemoryAppendOnlyInputTable.of(header);
        assertThat(create1).isEqualTo(create1);
        assertThat(create1).isNotEqualTo(create2);
    }

    @Test
    void specNotEquals() {
        final TableSpec spec = TableSpec.empty(0).view("Foo=1");
        final TableSpec create1 = InMemoryAppendOnlyInputTable.of(spec);
        final TableSpec create2 = InMemoryAppendOnlyInputTable.of(spec);
        assertThat(create1).isEqualTo(create1);
        assertThat(create1).isNotEqualTo(create2);
    }

    @Test
    void headerDepth() {
        final TableHeader header = TableHeader.of(ColumnHeader.ofInt("Foo"));
        final TableSpec create = InMemoryAppendOnlyInputTable.of(header);
        assertThat(create.depth()).isEqualTo(0);
    }

    @Test
    void specDepth() {
        final TableSpec spec = TableSpec.empty(0).view("Foo=1");
        final TableSpec create = InMemoryAppendOnlyInputTable.of(spec);
        assertThat(create.depth()).isGreaterThan(0);
    }
}
