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

public class TableCreationImplTest {

    // todo: this test will need to be replaced w/ a saner generator, or real examples, once we have
    // stricter query validation

    private static final List<Table> SOURCE_TABLES =
        Arrays.asList(NewTable.of(Column.of("Foo", 0, -1, 1, 42, null, 1, Integer.MAX_VALUE)),
            NewTable.of(Column.of("Bar", 0L, -1L, 1L, 42L, null, 1L, Long.MAX_VALUE)),
            Table.empty(0), Table.empty(1), Table.empty(100), Table.empty(0));

    private static final List<Function<Table, Table>> SINGLE_PARENT_OPS =
        Arrays.asList(TableCreationImplTest::head1, TableCreationImplTest::headMax,
            TableCreationImplTest::tail1, TableCreationImplTest::tailMax,
            TableCreationImplTest::whereFooEq1, TableCreationImplTest::whereFooEqTest,
            TableCreationImplTest::whereFooIsNull, TableCreationImplTest::viewFoo,
            TableCreationImplTest::viewFooPlus1, TableCreationImplTest::viewFooEqBar,
            TableCreationImplTest::updateViewFoo, TableCreationImplTest::updateViewFooPlus1,
            TableCreationImplTest::updateViewFooEqBar, TableCreationImplTest::updateFoo,
            TableCreationImplTest::updateFooPlus1, TableCreationImplTest::updateFooEqBar,
            TableCreationImplTest::selectFoo, TableCreationImplTest::selectFooPlus1,
            TableCreationImplTest::selectFooEqBar, TableCreationImplTest::selectAll);

    private static final List<BiFunction<Table, Table, Table>> DUAL_TABLE_OPS =
        Arrays.asList(TableCreationImplTest::naturalJoin1, TableCreationImplTest::naturalJoin2,
            TableCreationImplTest::naturalJoin3, TableCreationImplTest::naturalJoin4,
            TableCreationImplTest::exactJoin1, TableCreationImplTest::exactJoin2,
            TableCreationImplTest::exactJoin3, TableCreationImplTest::exactJoin4);


    static Table head1(Table table) {
        return table.head(1);
    }

    static Table headMax(Table table) {
        return table.head(Long.MAX_VALUE);
    }

    static Table tail1(Table table) {
        return table.tail(1);
    }

    static Table tailMax(Table table) {
        return table.tail(Long.MAX_VALUE);
    }

    static Table whereFooEq1(Table table) {
        return table.where("Foo=1");
    }

    static Table whereFooEqTest(Table table) {
        return table.where("Foo=`test`");
    }

    static Table whereFooIsNull(Table table) {
        return table.where("isNull(Foo)");
    }

    static Table viewFoo(Table table) {
        return table.view("Foo");
    }

    static Table viewFooPlus1(Table table) {
        return table.view("Foo=Foo+1");
    }

    static Table viewFooEqBar(Table table) {
        return table.view("Foo=Bar");
    }

    static Table updateViewFoo(Table table) {
        return table.updateView("Foo");
    }

    static Table updateViewFooPlus1(Table table) {
        return table.updateView("Foo=Foo+1");
    }

    static Table updateViewFooEqBar(Table table) {
        return table.updateView("Foo=Bar");
    }

    static Table updateFoo(Table table) {
        return table.update("Foo");
    }

    static Table updateFooPlus1(Table table) {
        return table.update("Foo=Foo+1");
    }

    static Table updateFooEqBar(Table table) {
        return table.update("Foo=Bar");
    }

    static Table selectFoo(Table table) {
        return table.select("Foo");
    }

    static Table selectFooPlus1(Table table) {
        return table.select("Foo=Foo+1");
    }

    static Table selectFooEqBar(Table table) {
        return table.select("Foo=Bar");
    }

    static Table selectAll(Table table) {
        return table.select();
    }

    static Table naturalJoin1(Table left, Table right) {
        return left.naturalJoin(right, "Foo");
    }

    static Table naturalJoin2(Table left, Table right) {
        return left.naturalJoin(right, "Foo", "Bar");
    }

    static Table naturalJoin3(Table left, Table right) {
        return left.naturalJoin(right, "Foo,Bar", "Baz");
    }

    static Table naturalJoin4(Table left, Table right) {
        return left.naturalJoin(right, "Foo", "Bar,Baz");
    }

    static Table exactJoin1(Table left, Table right) {
        return left.exactJoin(right, "Foo");
    }

    static Table exactJoin2(Table left, Table right) {
        return left.exactJoin(right, "Foo", "Bar");
    }

    static Table exactJoin3(Table left, Table right) {
        return left.exactJoin(right, "Foo,Bar", "Baz");
    }

    static Table exactJoin4(Table left, Table right) {
        return left.exactJoin(right, "Foo", "Bar,Baz");
    }

    static List<Table> createTables() {
        List<Table> tables = new ArrayList<>();
        for (Table sourceTable : SOURCE_TABLES) {
            tables.add(sourceTable);
            for (Function<Table, Table> op1 : SINGLE_PARENT_OPS) {
                Table inner1 = op1.apply(sourceTable);
                tables.add(inner1);
                for (Function<Table, Table> op2 : SINGLE_PARENT_OPS) {
                    Table inner2 = op2.apply(inner1);
                    tables.add(inner2);
                    for (BiFunction<Table, Table, Table> dualTableOp : DUAL_TABLE_OPS) {
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
        for (Table table : createTables()) {
            // this is really a test of TableCreationAdapterImpl and QST TableOperations impl
            assertThat(TableCreationImpl.toTable(table)).isEqualTo(table);
        }
    }
}
