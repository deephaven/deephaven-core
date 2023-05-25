/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Provides methods for building source tables.
 *
 * @param <TABLE> the table type
 */
public interface TableCreator<TABLE> {

    /**
     * "Replay" the {@code table} against the given interfaces.
     *
     * @param <TOPS> the table operations type
     * @param <TABLE> the output table type
     * @param creation the table creation
     * @param toOps the table to operations
     * @param toTable the operations to table
     * @param table the table specification
     * @return the output results
     */
    static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> TableAdapterResults<TOPS, TABLE> create(
            TableCreator<TABLE> creation, TableToOperations<TOPS, TABLE> toOps,
            OperationsToTable<TOPS, TABLE> toTable, TableSpec table) {
        return TableAdapterImpl.of(creation, toOps, toTable, table);
    }

    /**
     * "Replay" the {@code table} against the given interfaces.
     *
     * @param <TOPS> the table operations type
     * @param <TABLE> the output table type
     * @param creation the table creation
     * @param toOps the table to operations
     * @param toTable the operations to table
     * @param tables the table specifications
     * @return the output results
     */
    static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> TableAdapterResults<TOPS, TABLE> create(
            TableCreator<TABLE> creation, TableToOperations<TOPS, TABLE> toOps,
            OperationsToTable<TOPS, TABLE> toTable, Iterable<TableSpec> tables) {
        return TableAdapterImpl.of(creation, toOps, toTable, tables);
    }

    /**
     * Creates a new table.
     *
     * @param newTable the new table specification
     * @return the new table
     */
    TABLE of(NewTable newTable);

    /**
     * Creates an empty table.
     *
     * @param emptyTable the empty table specification
     * @return the empty table
     */
    TABLE of(EmptyTable emptyTable);

    /**
     * Creates a time table.
     *
     * @param timeTable the time table specifications
     * @return the time table
     */
    TABLE of(TimeTable timeTable);

    /**
     * Creates a ticket table.
     *
     * @param ticketTable the ticket table
     * @return the ticket table
     */
    TABLE of(TicketTable ticketTable);

    /**
     * Creates an input table.
     *
     * @param inputTable the input table specifications
     * @return the input table
     */
    TABLE of(InputTable inputTable);

    /**
     * Merges the given {@code tables}.
     *
     * @param tables the tables
     * @return the merged results
     * @see io.deephaven.qst.table.MergeTable
     */
    TABLE merge(Iterable<TABLE> tables);

    /**
     * Equivalent to {@code of(EmptyTable.of(size))}.
     *
     * @param size the size
     * @return the empty table
     * @see EmptyTable#of(long)
     */
    default TABLE emptyTable(long size) {
        return of(EmptyTable.of(size));
    }

    /**
     * Equivalent to {@code of(NewTable.of(columns))}.
     *
     * @param columns the columns
     * @return the new table
     * @see NewTable#of(Column[])
     */
    default TABLE newTable(Column<?>... columns) {
        return of(NewTable.of(columns));
    }

    /**
     * Equivalent to {@code of(NewTable.of(columns))}.
     *
     * @param columns the columns
     * @return the new table
     * @see NewTable#of(Iterable)
     */
    default TABLE newTable(Iterable<Column<?>> columns) {
        return of(NewTable.of(columns));
    }

    /**
     * Equivalent to {@code of(TimeTable.of(interval))}.
     *
     * @param interval the interval
     * @return the time table
     * @see TimeTable#of(Duration)
     */
    default TABLE timeTable(Duration interval) {
        return of(TimeTable.of(interval));
    }

    /**
     * Equivalent to {@code of(TimeTable.of(interval, startTime))}.
     *
     * @param interval the interval
     * @param startTime the start time
     * @return the time table
     * @see TimeTable#of(Duration)
     */
    default TABLE timeTable(Duration interval, Instant startTime) {
        return of(TimeTable.of(interval, startTime));
    }

    // Note: there isn't a good way to deal with generic vararg pollution. The annotation
    // @SafeVarargs doesn't work on non-final methods. To minimize warnings in generic client code,
    // we are templating out the merge methods; only falling back to varargs if the code is
    // supplying ten or more tables.

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE t1, TABLE t2) {
        return merge(Arrays.asList(t1, t2));
    }

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE t1, TABLE t2, TABLE t3) {
        return merge(Arrays.asList(t1, t2, t3));
    }

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE t1, TABLE t2, TABLE t3, TABLE t4) {
        return merge(Arrays.asList(t1, t2, t3, t4));
    }

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE t1, TABLE t2, TABLE t3, TABLE t4, TABLE t5) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5));
    }

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE t1, TABLE t2, TABLE t3, TABLE t4, TABLE t5, TABLE t6) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5, t6));
    }

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE t1, TABLE t2, TABLE t3, TABLE t4, TABLE t5, TABLE t6, TABLE t7) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5, t6, t7));
    }

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE t1, TABLE t2, TABLE t3, TABLE t4, TABLE t5, TABLE t6, TABLE t7,
            TABLE t8) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8));
    }

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE t1, TABLE t2, TABLE t3, TABLE t4, TABLE t5, TABLE t6, TABLE t7,
            TABLE t8, TABLE t9) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8, t9));
    }

    /**
     * @see #merge(Iterable)
     */
    @SuppressWarnings("unchecked")
    default TABLE merge(TABLE t1, TABLE t2, TABLE t3, TABLE t4, TABLE t5, TABLE t6, TABLE t7,
            TABLE t8, TABLE t9, TABLE... remaining) {
        return merge(
                () -> Stream.concat(Stream.of(t1, t2, t3, t4, t5, t6, t7, t8, t9), Arrays.stream(remaining))
                        .iterator());
    }

    /**
     * @see #merge(Iterable)
     */
    default TABLE merge(TABLE[] tables) {
        return merge(Arrays.asList(tables));
    }

    /**
     * Equivalent to {@code of(TicketTable.of(ticket))}.
     *
     * @param ticket the ticket string
     * @return the ticket table
     * @see TicketTable#of(String)
     */
    default TABLE ticket(String ticket) {
        return of(TicketTable.of(ticket));
    }

    /**
     * Equivalent to {@code of(TicketTable.of(ticket))}.
     *
     * @param ticket the ticket
     * @return the ticket table
     * @see TicketTable#of(byte[])
     */
    default TABLE ticket(byte[] ticket) {
        return of(TicketTable.of(ticket));
    }

    /**
     * Transform a table operation into a table.
     *
     * @param <TOPS> the table operations type
     * @param <TABLE> the output table type
     * @see #create(TableCreator, TableToOperations, OperationsToTable, TableSpec)
     */
    @FunctionalInterface
    interface OperationsToTable<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

        TABLE of(TOPS tableOperations);
    }

    /**
     * Transform a table into a table operation.
     *
     * @param <TOPS> the table operations type
     * @param <TABLE> the output table type
     * @see #create(TableCreator, TableToOperations, OperationsToTable, TableSpec)
     */
    @FunctionalInterface
    interface TableToOperations<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

        TOPS of(TABLE table);
    }
}
