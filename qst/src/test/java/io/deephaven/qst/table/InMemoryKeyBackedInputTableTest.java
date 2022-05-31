package io.deephaven.qst.table;

import io.deephaven.qst.column.header.ColumnHeader;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryKeyBackedInputTableTest {
    @Test
    void headerNotEquals() {
        final TableHeader header = TableHeader.of(ColumnHeader.ofInt("Foo"));
        final TableSpec create1 = InMemoryKeyBackedInputTable.of(header, Collections.emptyList());
        final TableSpec create2 = InMemoryKeyBackedInputTable.of(header, Collections.emptyList());
        assertThat(create1).isEqualTo(create1);
        assertThat(create1).isNotEqualTo(create2);
    }

    @Test
    void specNotEquals() {
        final TableSpec spec = TableSpec.empty(0).view("Foo=1");
        final TableSpec create1 = InMemoryKeyBackedInputTable.of(spec, Collections.emptyList());
        final TableSpec create2 = InMemoryKeyBackedInputTable.of(spec, Collections.emptyList());
        assertThat(create1).isEqualTo(create1);
        assertThat(create1).isNotEqualTo(create2);
    }

    @Test
    void headerDepth() {
        final TableHeader header = TableHeader.of(ColumnHeader.ofInt("Foo"));
        final TableSpec create = InMemoryKeyBackedInputTable.of(header, Collections.emptyList());
        assertThat(create.depth()).isEqualTo(0);
    }

    @Test
    void specDepth() {
        final TableSpec spec = TableSpec.empty(0).view("Foo=1");
        final TableSpec create = InMemoryKeyBackedInputTable.of(spec, Collections.emptyList());
        assertThat(create.depth()).isGreaterThan(0);
    }
}
