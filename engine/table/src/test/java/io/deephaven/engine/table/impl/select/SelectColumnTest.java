//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.literal.Literal;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectColumnTest {
    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final Literal V42 = Literal.of(42L);

    @Test
    public void testSingleColumn() {
        expect(FOO, SourceColumn.class, "Foo=Foo");
    }

    @Test
    public void testTwoColumns() {
        expect(Selectable.of(FOO, BAR), SourceColumn.class, "Foo=Bar");
    }

    @Test
    public void testSelectLong() {
        expect(Selectable.of(FOO, V42), SwitchColumn.class, "Foo=42L");
    }

    @Test
    public void testRawString() {
        expect(Selectable.of(FOO, RawString.of("foo(X-13)")), SwitchColumn.class, "Foo=foo(X-13)");
    }

    private static void expect(Selectable selectable, Class<? extends SelectColumn> clazz, String expected) {
        SelectColumn impl = SelectColumn.of(selectable);
        assertThat(impl).isInstanceOf(clazz);
        // SelectColumn doesn't necessary implement equals, so we need to use the string repr
        assertThat(impl.toString()).isEqualTo(expected);
    }
}
