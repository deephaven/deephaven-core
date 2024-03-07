//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.literal.Literal;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class WhereSessionTest extends DeephavenSessionTestBase {

    public static int myFunction() {
        return 42;
    }

    @Test
    public void allowStaticI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), "i % 2 == 0");
    }

    @Test
    public void allowStaticII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), "ii % 2 == 0");
    }

    @Test
    public void allowTimeTableI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)), "i % 2 == 0");
    }

    @Test
    public void allowTimeTableII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)), "ii % 2 == 0");
    }

    @Test
    public void disallowCustomFunctions() throws InterruptedException {
        disallow(TableSpec.empty(1), String.format("%s.myFunction() == 42", WhereSessionTest.class.getName()));
    }

    @Test
    public void disallowNew() throws InterruptedException {
        disallow(TableSpec.empty(1), "new Object() == 42");
    }

    @Test
    public void allowTopLevelIn() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1).view("I=ii"), "I in 0, 1", "I > 37");
    }

    @Test
    public void disallowNestedIn() throws InterruptedException, TableHandle.TableHandleException {
        disallow(TableSpec.empty(1).view("I=ii"), Filter.or(Filter.from("I in 0, 1", "I > 37")));
    }

    @Test
    public void allowTrue() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), Filter.ofTrue());
    }

    @Test
    public void allowFalse() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), Filter.ofFalse());
    }

    @Test
    public void allowInSimple() throws TableHandleException, InterruptedException {
        final FilterIn filter = FilterIn.of(ColumnName.of("Foo"), Literal.of("Foo"));
        allow(TableSpec.empty(1).view("Foo=`Foo`", "Bar=`Bar`").where(filter));
    }

    @Test
    public void allowIn() throws TableHandleException, InterruptedException {
        final FilterIn filter = FilterIn.of(ColumnName.of("Foo"), Literal.of("Foo"), Literal.of("FooBar"));
        allow(TableSpec.empty(1).view("Foo=`Foo`", "Bar=`Bar`").where(filter));
    }

    @Test
    public void allowNotInSimple() throws TableHandleException, InterruptedException {
        final FilterIn filter = FilterIn.of(ColumnName.of("Foo"), Literal.of("Foo"));
        allow(TableSpec.empty(1).view("Foo=`Foo`", "Bar=`Bar`").where(Filter.not(filter)));
    }

    @Test
    public void allowNotIn() throws TableHandleException, InterruptedException {
        final FilterIn filter = FilterIn.of(ColumnName.of("Foo"), Literal.of("Foo"), Literal.of("FooBar"));
        allow(TableSpec.empty(1).view("Foo=`Foo`", "Bar=`Bar`").where(Filter.not(filter)));
    }

    @Test
    public void allowEqComparison() throws TableHandleException, InterruptedException {
        final FilterComparison filter = FilterComparison.eq(ColumnName.of("Foo"), Literal.of("Foo"));
        allow(TableSpec.empty(1).view("Foo=`Foo`", "Bar=`Bar`").where(filter));
    }

    @Test
    public void allowNeqComparison() throws TableHandleException, InterruptedException {
        final FilterComparison filter = FilterComparison.neq(ColumnName.of("Foo"), Literal.of("Foo"));
        allow(TableSpec.empty(1).view("Foo=`Foo`", "Bar=`Bar`").where(filter));
    }

    @Test
    public void allowNotEqComparison() throws TableHandleException, InterruptedException {
        final FilterComparison filter = FilterComparison.eq(ColumnName.of("Foo"), Literal.of("Foo"));
        allow(TableSpec.empty(1).view("Foo=`Foo`", "Bar=`Bar`").where(Filter.not(filter)));
    }

    @Test
    public void allowNotNeqComparison() throws TableHandleException, InterruptedException {
        final FilterComparison filter = FilterComparison.neq(ColumnName.of("Foo"), Literal.of("Foo"));
        allow(TableSpec.empty(1).view("Foo=`Foo`", "Bar=`Bar`").where(Filter.not(filter)));
    }

    private void allow(TableSpec parent, String... filters)
            throws InterruptedException, TableHandle.TableHandleException {
        allow(parent, Filter.and(Filter.from(filters)));
    }

    private void disallow(TableSpec parent, String... filters) throws InterruptedException {
        disallow(parent, Filter.and(Filter.from(filters)));
    }

    private void allow(TableSpec parent, Filter filter) throws InterruptedException, TableHandle.TableHandleException {
        try (final TableHandle handle = session.batch().execute(parent.where(filter))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    private void disallow(TableSpec parent, Filter filter) throws InterruptedException {
        try (final TableHandle handle = session.batch().execute(parent.where(filter))) {
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (TableHandle.TableHandleException e) {
            // expected
        }
    }
}

