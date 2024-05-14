//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.Graphviz;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.type.Type;
import org.apache.calcite.runtime.CalciteContextException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SqlAdapterTest {

    private static final TableHeader AUTHORS = TableHeader.of(
            ColumnHeader.ofInt("Id"),
            ColumnHeader.ofString("Name"));

    private static final TableHeader BOOKS = TableHeader.of(
            ColumnHeader.ofInt("Id"),
            ColumnHeader.ofString("Title"),
            ColumnHeader.ofInt("AuthorId"));

    private static final TableHeader LONG_I = TableHeader.of(ColumnHeader.ofLong("I"));

    private static final TableHeader TIB = TableHeader.of(
            ColumnHeader.ofInstant("Timestamp"),
            ColumnHeader.ofLong("I"),
            ColumnHeader.ofBoolean("B"));

    private static final TableHeader TIR = TableHeader.of(
            ColumnHeader.ofInstant("Timestamp"),
            ColumnHeader.ofLong("I"),
            ColumnHeader.ofDouble("R"));

    private static final TableHeader TIME1 = TableHeader.of(ColumnHeader.ofInstant("Time1"));

    private static final TableHeader TIME2 = TableHeader.of(ColumnHeader.ofInstant("Time2"));

    interface MyCustomType {

    }

    private static final TableHeader CUSTOM = TableHeader.of(
            ColumnHeader.of("Foo", Type.longType()),
            ColumnHeader.of("Bar", Type.ofCustom(MyCustomType.class)));

    @Test
    void sql1() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "authors", AUTHORS,
                "books", BOOKS);
        check(scope, 1);
    }

    @Test
    void sql2() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "authors", AUTHORS,
                "books", BOOKS);
        check(scope, 2);
    }

    @Test
    void sql3() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 3);
    }

    @Test
    void sql4() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 4);
    }

    @Test
    void sql5() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 5);
    }

    @Test
    void sql6() throws IOException, URISyntaxException {
        final Scope scope = scope("longi", LONG_I);
        check(scope, 6);
    }

    @Test
    void sql7() throws IOException, URISyntaxException {
        final Scope scope = scope("longi", LONG_I);
        check(scope, 7);
    }

    @Test
    void sql8() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "authors", AUTHORS,
                "books", BOOKS);
        check(scope, 8);
    }

    @Test
    void sql9() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 9);
    }

    @Test
    void sql10() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 10);
    }

    @Test
    void sql11() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 11);
    }

    @Test
    void sql12() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 12);
    }

    @Test
    void sql13() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 13);
    }

    @Test
    void sql14() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 14);
    }

    @Test
    void sql15() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 15);
    }

    @Test
    void sql16() throws IOException, URISyntaxException {
        final Scope scope = scope("my_time", TIB);
        check(scope, 16);
    }

    @Test
    void sql17() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "time_1", TIR,
                "time_2", TIR,
                "time_3", TIR);
        check(scope, 17);
    }

    @Test
    void sql18() throws IOException, URISyntaxException {
        final Scope scope = scope("my_time", TIB);
        check(scope, 18);
    }

    @Test
    void sql19() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "my_time_1", TIME1,
                "my_time_2", TIME2);
        check(scope, 19);
    }

    @Test
    void sql20() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "my_time_1", TIME1,
                "my_time_2", TIME2);
        check(scope, 20);
    }

    @Test
    void sql21() throws IOException, URISyntaxException {
        check(scope(), 21);
    }

    @Test
    void sql22() throws IOException, URISyntaxException {
        check(scope(), 22);
    }

    @Test
    void sql23() throws IOException, URISyntaxException {
        check(scope(), 23);
    }

    @Test
    void sql24() throws IOException, URISyntaxException {
        check(scope(), 24);
    }

    @Test
    void sql25() throws IOException, URISyntaxException {
        final Scope scope = scope("my_time", TIB);
        check(scope, 25);
    }

    @Test
    void sql26() throws IOException, URISyntaxException {
        final Scope scope = scope("my_time", TIB);
        check(scope, 26);
    }

    @Test
    void sql27() throws IOException, URISyntaxException {
        final Scope scope = scope("my_time", TIB);
        check(scope, 27);
    }

    @Test
    void sql28() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "authors", AUTHORS,
                "books", BOOKS);
        check(scope, 28);
    }

    @Test
    void sql29() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "authors", AUTHORS,
                "books", BOOKS);
        check(scope, 29);
    }

    @Test
    void sql30() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 30);
    }

    @Test
    void sql31() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 31);
    }

    @Test
    void sql32() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 32);
    }

    @Test
    void sql33() throws IOException, URISyntaxException {
        final Scope scope = scope("books", BOOKS);
        check(scope, 33);
    }

    @Test
    void sql34() throws IOException, URISyntaxException {
        final Scope scope = scope(
                "authors", AUTHORS,
                "books", BOOKS);
        check(scope, 34);
    }

    @Test
    void sql35() throws IOException, URISyntaxException {
        final Scope scope = scope();
        check(scope, 35);
    }

    @Test
    void sql36() throws IOException, URISyntaxException {
        final Scope scope = scope("custom", CUSTOM);
        check(scope, 36);
    }

    @Test
    void sql37() throws IOException, URISyntaxException {
        final Scope scope = scope("custom", CUSTOM);
        check(scope, 37);
    }

    @Test
    void sql38() throws IOException, URISyntaxException {
        final Scope scope = scope("custom", CUSTOM);
        check(scope, 38);
    }

    @Test
    void sql39() throws IOException, URISyntaxException {
        final Scope scope = scope("custom", CUSTOM);
        checkError(scope, 39, CalciteContextException.class,
                "Cannot apply '+' to arguments of type '<JAVATYPE(CLASS IO.DEEPHAVEN.SQL.TYPEADAPTER$SQLTODOCUSTOMTYPE)> + <INTEGER>'");
    }

    private static void check(Scope scope, int index) throws IOException, URISyntaxException {
        check(scope, String.format("query-%d.sql", index), String.format("qst-%d.dot", index));
    }

    private static void checkError(Scope scope, int index, Class<? extends Throwable> clazz, String messagePart)
            throws IOException, URISyntaxException {
        try {
            SqlAdapter.parseSql(read(String.format("query-%d.sql", index)), scope);
            failBecauseExceptionWasNotThrown(clazz);
        } catch (Throwable t) {
            assertThat(t).isInstanceOf(clazz);
            assertThat(t).hasMessageContaining(messagePart);
        }
    }

    private static void check(Scope scope, String queryResource, String expectedResource)
            throws IOException, URISyntaxException {
        checkSql(expectedResource, read(queryResource), scope);
    }

    private static void checkSql(String expectedResource, String sql, Scope scope)
            throws IOException, URISyntaxException {
        final TableSpec results = SqlAdapter.parseSql(sql, scope);
        assertThat(Graphviz.toDot(results)).isEqualTo(read(expectedResource));
        // Note: we are *abusing* toDot() / postOrderWalk(), as we are assuming a stable order but the docs specifically
        // say not to do that. Since we control the implementation, we can change the implementation to leverage the
        // "stability" until we have a proper ser/deser format (json?) that we can use for TableSpec. The alternative is
        // to manually create the TableSpecs in-code - which is possible, but would be tedious and verbose.
        //
        // Additionally, the dot format is somewhat human readable, and provides a good avenue for visualization
        // purposes during the development and testing of SqlAdapter.
        //
        // assertThat(results).isEqualTo(readJsonToTableSpec(expectedResource));
    }

    private static String read(String resourceName) throws IOException, URISyntaxException {
        return Files.readString(Path.of(SqlAdapterTest.class.getResource(resourceName).toURI()));
    }

    private static Scope scope() {
        return ScopeStaticImpl.empty();
    }

    private static TicketTable scan(String name) {
        return TicketTable.of(("scan/" + name).getBytes(StandardCharsets.UTF_8));
    }

    private static Scope scope(String name, TableHeader header) {
        return ScopeStaticImpl.builder()
                .addTables(TableInformation.of(List.of(name), header, scan(name)))
                .build();
    }

    private static Scope scope(
            String name1, TableHeader header1,
            String name2, TableHeader header2) {
        return ScopeStaticImpl.builder()
                .addTables(
                        TableInformation.of(List.of(name1), header1, scan(name1)),
                        TableInformation.of(List.of(name2), header2, scan(name2)))
                .build();
    }

    private static Scope scope(
            String name1, TableHeader header1,
            String name2, TableHeader header2,
            String name3, TableHeader header3) {
        return ScopeStaticImpl.builder()
                .addTables(
                        TableInformation.of(List.of(name1), header1, scan(name1)),
                        TableInformation.of(List.of(name2), header2, scan(name2)),
                        TableInformation.of(List.of(name3), header3, scan(name3)))
                .build();
    }
}
