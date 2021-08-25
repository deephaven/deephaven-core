package io.deephaven.web.client.api.filter;

import com.google.gwt.junit.client.GWTTestCase;
import io.deephaven.web.client.api.Column;

/**
 * Tests basic construction of filter condition instances from simple tables. This does not fully
 * end-to-end test the filter, just the API around the simple AST we use, especially validation.
 */
public class FilterConditionTestGwt extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DhApiDev";
    }

    private Column getColumn() {
        return new Column(0, 0, -1, -1, "int", "ColumnName", false, -1, null);
    }

    private FilterValue[] arr(FilterValue filterValue) {
        return new FilterValue[] {filterValue};
    }

    public void testCreateSimpleFilters() {
        Column c = getColumn();

        assertEquals("ColumnName == (ignore case) 1",
            c.filter().eqIgnoreCase(FilterValue.ofNumber(1)).toString());
        assertEquals("ColumnName != (ignore case) 1",
            c.filter().notEqIgnoreCase(FilterValue.ofNumber(1)).toString());

        assertEquals("ColumnName == 1", c.filter().eq(FilterValue.ofNumber(1)).toString());
        assertEquals("ColumnName != 1", c.filter().notEq(FilterValue.ofNumber(1)).toString());
        assertEquals("ColumnName > 1", c.filter().greaterThan(FilterValue.ofNumber(1)).toString());
        assertEquals("ColumnName < 1", c.filter().lessThan(FilterValue.ofNumber(1)).toString());
        assertEquals("ColumnName >= 1",
            c.filter().greaterThanOrEqualTo(FilterValue.ofNumber(1)).toString());
        assertEquals("ColumnName <= 1",
            c.filter().lessThanOrEqualTo(FilterValue.ofNumber(1)).toString());

        assertEquals("ColumnName in 1", c.filter().in(arr(FilterValue.ofNumber(1))).toString());
        assertEquals("ColumnName not in 1",
            c.filter().notIn(arr(FilterValue.ofNumber(1))).toString());
        assertEquals("ColumnName icase in 1",
            c.filter().inIgnoreCase(arr(FilterValue.ofNumber(1))).toString());
        assertEquals("ColumnName icase not in 1",
            c.filter().notInIgnoreCase(arr(FilterValue.ofNumber(1))).toString());

        assertEquals("ColumnName == true", c.filter().isTrue().toString());
        assertEquals("ColumnName == false", c.filter().isFalse().toString());
        assertEquals("isNull(ColumnName)", c.filter().isNull().toString());

        assertEquals("ColumnName.foo1()", c.filter().invoke("foo1").toString());
        assertEquals("ColumnName.foo2(1)",
            c.filter().invoke("foo2", FilterValue.ofNumber(1)).toString());
        assertEquals("ColumnName.foo3(1, 2, \"three\")",
            c.filter().invoke("foo3", FilterValue.ofNumber(1), FilterValue.ofNumber(2),
                FilterValue.ofString("three")).toString());

        assertEquals("foo4()", FilterCondition.invoke("foo4").toString());
        assertEquals("foo5(1)", FilterCondition.invoke("foo5", FilterValue.ofNumber(1)).toString());
        assertEquals("foo6(1, 2, \"three\")",
            FilterCondition.invoke("foo6", FilterValue.ofNumber(1), FilterValue.ofNumber(2),
                FilterValue.ofString("three")).toString());
    }

    public void testCreateCombinedFilters() {
        Column c = getColumn();

        // individual AND
        assertEquals("(ColumnName == 1 && ColumnName != 2)",
            c.filter().eq(FilterValue.ofNumber(1)).and(c.filter().notEq(FilterValue.ofNumber(2)))
                .toString());

        // individual OR
        assertEquals("(ColumnName == 1 || ColumnName != 2)",
            c.filter().eq(FilterValue.ofNumber(1)).or(c.filter().notEq(FilterValue.ofNumber(2)))
                .toString());

        // individual NOT
        assertEquals("!(ColumnName == 1)", c.filter().eq(FilterValue.ofNumber(1)).not().toString());

        // nested/combined
        assertEquals(
            "(ColumnName == 1 && !((ColumnName == 2 || ColumnName == 3 || ColumnName == 4)))",
            c.filter().eq(FilterValue.ofNumber(1)).and(
                c.filter().eq(FilterValue.ofNumber(2))
                    .or(
                        c.filter().eq(FilterValue.ofNumber(3)),
                        c.filter().eq(FilterValue.ofNumber(4)))
                    .not())
                .toString()

        );
    }

}
