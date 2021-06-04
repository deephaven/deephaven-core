package io.deephaven.qst.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import io.deephaven.qst.table.column.Column;
import io.deephaven.qst.table.column.header.ColumnHeader;
import org.junit.jupiter.api.Test;

public class NewTableTest {

    @Test
    public void checkColumnSize() {
        try {
            NewTable.of(
                Column.of("Size1", 1),
                Column.of("Size2", 1, 2));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void checkDistinctNames() {
        try {
            NewTable.of(
                Column.of("Size1", 1),
                Column.of("Size1", 2));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void newTableHelperColumnOriented() {
        NewTable expected = ImmutableNewTable.builder()
            .size(3)
            .addColumns(Column.builder(ColumnHeader.ofInt("X"))
                .add(1).add(null).add(3)
                .build())
            .build();

        NewTable actual = NewTable.of(Column.of("X", 1, null, 3));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void newTableHelperRowOriented() {

        Column<Integer> x = Column.builder(ColumnHeader.ofInt("X"))
            .add(1).add(2)
            .build();

        Column<String> y = Column.builder(ColumnHeader.ofString("Y"))
            .add("one").add("two")
            .build();

        NewTable expected = ImmutableNewTable.builder()
            .size(2)
            .addColumns(x, y)
            .build();

        NewTable actual = NewTable
            .header("X", int.class)
            .header("Y", String.class)
            .row(1, "one")
            .row(2, "two")
            .build();

        assertThat(actual).isEqualTo(expected);
    }
}
