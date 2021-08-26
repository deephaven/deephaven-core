package io.deephaven.qst.table;

import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.qst.examples.EmployeesExample;
import io.deephaven.qst.column.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class TableCreatorImplTest {

    // todo: this test will need to be replaced w/ a saner generator, or real examples, once we have
    // stricter query validation

    private static final List<TableSpec> SOURCE_TABLES =
            Arrays.asList(NewTable.of(Column.of("Foo", 0, -1, 1, 42, null, 1, Integer.MAX_VALUE)),
                    NewTable.of(Column.of("Bar", 0L, -1L, 1L, 42L, null, 1L, Long.MAX_VALUE)),
                    TableSpec.empty(0), TableSpec.empty(1), TableSpec.empty(100), TableSpec.empty(0));

    private static final List<Function<TableSpec, TableSpec>> SINGLE_PARENT_OPS =
            Arrays.asList(TableCreatorImplTest::head1, TableCreatorImplTest::headMax,
                    TableCreatorImplTest::tail1, TableCreatorImplTest::tailMax,
                    TableCreatorImplTest::whereFooEq1, TableCreatorImplTest::whereFooEqTest,
                    TableCreatorImplTest::whereFooIsNull, TableCreatorImplTest::viewFoo,
                    TableCreatorImplTest::viewFooPlus1, TableCreatorImplTest::viewFooEqBar,
                    TableCreatorImplTest::updateViewFoo, TableCreatorImplTest::updateViewFooPlus1,
                    TableCreatorImplTest::updateViewFooEqBar, TableCreatorImplTest::updateFoo,
                    TableCreatorImplTest::updateFooPlus1, TableCreatorImplTest::updateFooEqBar,
                    TableCreatorImplTest::selectFoo, TableCreatorImplTest::selectFooPlus1,
                    TableCreatorImplTest::selectFooEqBar, TableCreatorImplTest::selectAll);

    private static final List<BiFunction<TableSpec, TableSpec, TableSpec>> DUAL_TABLE_OPS =
            Arrays.asList(TableCreatorImplTest::naturalJoin1, TableCreatorImplTest::naturalJoin2,
                    TableCreatorImplTest::naturalJoin3, TableCreatorImplTest::naturalJoin4,
                    TableCreatorImplTest::exactJoin1, TableCreatorImplTest::exactJoin2,
                    TableCreatorImplTest::exactJoin3, TableCreatorImplTest::exactJoin4);


    static TableSpec head1(TableSpec table) {
        return table.head(1);
    }

    static TableSpec headMax(TableSpec table) {
        return table.head(Long.MAX_VALUE);
    }

    static TableSpec tail1(TableSpec table) {
        return table.tail(1);
    }

    static TableSpec tailMax(TableSpec table) {
        return table.tail(Long.MAX_VALUE);
    }

    static TableSpec whereFooEq1(TableSpec table) {
        return table.where("Foo=1");
    }

    static TableSpec whereFooEqTest(TableSpec table) {
        return table.where("Foo=`test`");
    }

    static TableSpec whereFooIsNull(TableSpec table) {
        return table.where("isNull(Foo)");
    }

    static TableSpec viewFoo(TableSpec table) {
        return table.view("Foo");
    }

    static TableSpec viewFooPlus1(TableSpec table) {
        return table.view("Foo=Foo+1");
    }

    static TableSpec viewFooEqBar(TableSpec table) {
        return table.view("Foo=Bar");
    }

    static TableSpec updateViewFoo(TableSpec table) {
        return table.updateView("Foo");
    }

    static TableSpec updateViewFooPlus1(TableSpec table) {
        return table.updateView("Foo=Foo+1");
    }

    static TableSpec updateViewFooEqBar(TableSpec table) {
        return table.updateView("Foo=Bar");
    }

    static TableSpec updateFoo(TableSpec table) {
        return table.update("Foo");
    }

    static TableSpec updateFooPlus1(TableSpec table) {
        return table.update("Foo=Foo+1");
    }

    static TableSpec updateFooEqBar(TableSpec table) {
        return table.update("Foo=Bar");
    }

    static TableSpec selectFoo(TableSpec table) {
        return table.select("Foo");
    }

    static TableSpec selectFooPlus1(TableSpec table) {
        return table.select("Foo=Foo+1");
    }

    static TableSpec selectFooEqBar(TableSpec table) {
        return table.select("Foo=Bar");
    }

    static TableSpec selectAll(TableSpec table) {
        return table.select();
    }

    static TableSpec naturalJoin1(TableSpec left, TableSpec right) {
        return left.naturalJoin(right, "Foo");
    }

    static TableSpec naturalJoin2(TableSpec left, TableSpec right) {
        return left.naturalJoin(right, "Foo", "Bar");
    }

    static TableSpec naturalJoin3(TableSpec left, TableSpec right) {
        return left.naturalJoin(right, "Foo,Bar", "Baz");
    }

    static TableSpec naturalJoin4(TableSpec left, TableSpec right) {
        return left.naturalJoin(right, "Foo", "Bar,Baz");
    }

    static TableSpec exactJoin1(TableSpec left, TableSpec right) {
        return left.exactJoin(right, "Foo");
    }

    static TableSpec exactJoin2(TableSpec left, TableSpec right) {
        return left.exactJoin(right, "Foo", "Bar");
    }

    static TableSpec exactJoin3(TableSpec left, TableSpec right) {
        return left.exactJoin(right, "Foo,Bar", "Baz");
    }

    static TableSpec exactJoin4(TableSpec left, TableSpec right) {
        return left.exactJoin(right, "Foo", "Bar,Baz");
    }

    static List<TableSpec> createTables() {
        List<TableSpec> tables = new ArrayList<>();
        for (TableSpec sourceTable : SOURCE_TABLES) {
            tables.add(sourceTable);
            for (Function<TableSpec, TableSpec> op1 : SINGLE_PARENT_OPS) {
                TableSpec inner1 = op1.apply(sourceTable);
                tables.add(inner1);
                for (Function<TableSpec, TableSpec> op2 : SINGLE_PARENT_OPS) {
                    TableSpec inner2 = op2.apply(inner1);
                    tables.add(inner2);
                    for (BiFunction<TableSpec, TableSpec, TableSpec> dualTableOp : DUAL_TABLE_OPS) {
                        tables.add(dualTableOp.apply(inner1, inner2));
                        tables.add(dualTableOp.apply(inner2, inner1));
                    }
                }
            }
        }
        tables.add(EmployeesExample.joined());
        return tables;
    }

    @Test
    void equivalence() {
        for (TableSpec table : createTables()) {
            // this is really a test of TableCreationAdapterImpl and QST TableOperations impl
            assertThat(TableCreatorImpl.toTable(table)).isEqualTo(table);
        }
    }
}
