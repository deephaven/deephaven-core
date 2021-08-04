package io.deephaven.qst.table;

import io.deephaven.qst.column.Column;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class NewTableTest {

    @Test
    public void checkColumnSize() {
        try {
            NewTable.of(Column.of("Size1", 1), Column.of("Size2", 1, 2));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // The immutables builder pattern overwrites when there are duplicate map keys.
    // It would be nice to catch this type of error.
    // https://github.com/immutables/immutables/issues/1107
    // @Test
    // public void checkDistinctNames() {
    // try {
    // NewTable.of(Column.of("Size1", 1), Column.of("Size1", 2));
    // failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
    // } catch (IllegalArgumentException e) {
    // // expected
    // }
    // }
}
