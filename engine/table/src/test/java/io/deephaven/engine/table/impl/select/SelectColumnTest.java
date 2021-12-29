package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.value.Value;
import junit.framework.TestCase;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectColumnTest extends TestCase {
    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final Value V42 = Value.of(42L);

    public void testSingleColumn() {
        expect(FOO, SourceColumn.class, "Foo=Foo");
    }

    public void testTwoColumns() {
        expect(Selectable.of(FOO, BAR), SourceColumn.class, "Foo=Bar");
    }

    public void testSelectLong() {
        expect(Selectable.of(FOO, V42), SwitchColumn.class, "Foo=42L");
    }

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
