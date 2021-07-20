package io.deephaven.qst;

import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders3;

public class Examples4 {



    public static NewTable example1() {
        return NewTable.of(
            Column.of("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers"),
            Column.of("DeptId", 31, 33, 33, 34, 34, null), Column.of("Telephone", "(347) 555-0123",
                "(917) 555-0198", "(212) 555-0167", "(952) 555-0110", null, null));
    }

    // ---------------------------------------------------------------

    static NewTable example2() {
        return ColumnHeader.of("LastName", String.class).header("DeptId", int.class)
            .header("Telephone", String.class).row("Rafferty", 31, "(347) 555-0123")
            .row("Jones", 33, "(917) 555-0198").row("Steiner", 33, "(212) 555-0167")
            .row("Robins", 34, "(952) 555-0110").row("Smith", 34, null).row("Rogers", null, null)
            .build();
    }

    // ---------------------------------------------------------------

    static final ColumnHeader<Integer> DEPT_ID = ColumnHeader.ofInt("DeptId");
    static final ColumnHeader<String> TELEPHONE = ColumnHeader.ofString("Telephone");

    static final ColumnHeaders3<String, Integer, String> EMPLOYEE_HEADER =
        ColumnHeader.of("LastName", String.class).header(DEPT_ID).header(TELEPHONE);

    static final ColumnHeaders3<Integer, String, String> DEPARTMENT_HEADER =
        DEPT_ID.header("DeptName", String.class).header(TELEPHONE);

    static NewTable example3a() {
        return EMPLOYEE_HEADER.row("Rafferty", 31, "(347) 555-0123")
            .row("Jones", 33, "(917) 555-0198").row("Steiner", 33, "(212) 555-0167")
            .row("Robins", 34, "(952) 555-0110").row("Smith", 34, null).row("Rogers", null, null)
            .build();
    }

    static NewTable example3b() {
        return EMPLOYEE_HEADER.row("Foo", null, "(555) 555-5555").row("Bar", null, "(555) 555-5555")
            .build();
    }

    static NewTable example3c() {
        return DEPARTMENT_HEADER.row(31, "Sales", "(646) 555-0134")
            .row(33, "Engineering", "(646) 555-0178").row(34, "Clerical", "(646) 555-0159")
            .row(35, "Marketing", "(212) 555-0111").build();
    }

    // ---------------------------------------------------------------

    interface Employee {
        String name();

        Integer deptId();

        String telephone();
    }

    interface Department {
        int id();

        String name();

        String telephone();
    }

    // impls out-of-scope

    static Employee RAFFERTY = null;
    static Employee JONES = null;
    static Employee STEINER = null;
    static Employee ROBINS = null;
    static Employee SMITH = null;
    static Employee ROGERS = null;

    static Department SALES = null;
    static Department ENGINEERING = null;
    static Department CLERICAL = null;
    static Department MARKETING = null;

    static NewTable example4a(Employee... employees) {
        ColumnHeaders3<String, Integer, String>.Rows builder =
            EMPLOYEE_HEADER.start(employees.length);
        for (Employee employee : employees) {
            builder.row(employee.name(), employee.deptId(), employee.telephone());
        }
        return builder.build();
    }

    static NewTable example4a() {
        return example4a(RAFFERTY, JONES, STEINER, ROBINS, SMITH, ROGERS);
    }

    static NewTable example4b(Department... departments) {
        ColumnHeaders3<Integer, String, String>.Rows builder =
            DEPARTMENT_HEADER.start(departments.length);
        for (Department d : departments) {
            builder.row(d.id(), d.name(), d.telephone());
        }
        return builder.build();
    }

    static NewTable example4b() {
        return example4b(SALES, ENGINEERING, CLERICAL, MARKETING);
    }

    // ---------------------------------------------------------------

}
