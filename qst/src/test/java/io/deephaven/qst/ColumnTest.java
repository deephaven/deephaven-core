package io.deephaven.qst;

import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.qst.table.column.Column;
import io.deephaven.qst.table.column.header.ColumnHeader;
import org.junit.jupiter.api.Test;

public class ColumnTest {

    @Test
    public void intHelper() {
        Column<Integer> expected = Column.builder(ColumnHeader.ofInt("AnInt"))
            .add(1).add(null).add(3)
            .build();

        Column<Integer> actual = Column.of("AnInt", 1, null, 3);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void doubleHelper() {
        Column<Double> expected = Column.builder(ColumnHeader.ofDouble("ADouble"))
            .add(1.).add(null).add(3.)
            .build();

        Column<Double> actual = Column.of("ADouble", 1., null, 3.);

        assertThat(actual).isEqualTo(expected);
    }
}
