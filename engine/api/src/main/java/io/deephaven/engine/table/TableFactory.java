package io.deephaven.engine.table;

import io.deephaven.qst.TableCreator;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Factory for producing Deephaven engine Table instances.
 */
public class TableFactory {

    @FunctionalInterface
    public interface TableCreatorProvider {
        TableCreator<Table> get();
    }

    private static final class TableCreatorHolder {
        private static final TableCreator<Table> tableCreator =
                ServiceLoader.load(TableCreatorProvider.class).iterator().next().get();
    }

    private static TableCreator<Table> tableCreator() {
        return TableCreatorHolder.tableCreator;
    }

    /**
     * Creates a new table.
     *
     * @param newTable the new table specification
     * @return the new table
     */
    public static Table of(NewTable newTable) {
        return tableCreator().of(newTable);
    }

    /**
     * Creates an empty table.
     *
     * @param emptyTable the empty table specification
     * @return the empty table
     */
    public static Table of(EmptyTable emptyTable) {
        return tableCreator().of(emptyTable);
    }

    /**
     * Creates a time table.
     *
     * @param timeTable the time table specifications
     * @return the time table
     */
    public static Table of(TimeTable timeTable) {
        return tableCreator().of(timeTable);
    }

    /**
     * Creates a ticket table.
     *
     * @param ticketTable the ticket table
     * @return the ticket table
     */
    public static Table of(TicketTable ticketTable) {
        return tableCreator().of(ticketTable);
    }

    /**
     * Merges the given {@code tables}.
     *
     * @param tables the tables
     * @return the merged results
     * @see io.deephaven.qst.table.MergeTable
     */
    public static Table merge(Iterable<Table> tables) {
        return tableCreator().merge(tables);
    }

    /**
     * Equivalent to {@code of(EmptyTable.of(size))}.
     *
     * @param size the size
     * @return the empty table
     * @see EmptyTable#of(long)
     */
    public static Table emptyTable(long size) {
        return of(EmptyTable.of(size));
    }

    /**
     * Equivalent to {@code of(NewTable.of(columns))}.
     *
     * @param columns the columns
     * @return the new table
     * @see NewTable#of(Column[])
     */
    public static Table newTable(Column<?>... columns) {
        return of(NewTable.of(columns));
    }

    /**
     * Equivalent to {@code of(NewTable.of(columns))}.
     *
     * @param columns the columns
     * @return the new table
     * @see NewTable#of(Iterable)
     */
    public static Table newTable(Iterable<Column<?>> columns) {
        return of(NewTable.of(columns));
    }

    /**
     * Equivalent to {@code of(TimeTable.of(interval))}.
     *
     * @param interval the interval
     * @return the time table
     * @see TimeTable#of(Duration)
     */
    public static Table timeTable(Duration interval) {
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
    public static Table timeTable(Duration interval, Instant startTime) {
        return of(TimeTable.of(interval, startTime));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2) {
        return merge(Arrays.asList(t1, t2));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2, Table t3) {
        return merge(Arrays.asList(t1, t2, t3));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2, Table t3, Table t4) {
        return merge(Arrays.asList(t1, t2, t3, t4));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2, Table t3, Table t4, Table t5) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5, t6));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6, Table t7) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5, t6, t7));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6, Table t7,
            Table t8) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6, Table t7,
            Table t8, Table t9) {
        return merge(Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8, t9));
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6, Table t7,
            Table t8, Table t9, Table... remaining) {
        return merge(
                () -> Stream.concat(Stream.of(t1, t2, t3, t4, t5, t6, t7, t8, t9), Stream.of(remaining))
                        .iterator());
    }

    /**
     * @see #merge(Iterable)
     */
    public static Table merge(Table[] tables) {
        return merge(Arrays.asList(tables));
    }

    /**
     * Equivalent to {@code of(TicketTable.of(ticket))}.
     *
     * @param ticket the ticket string
     * @return the ticket table
     * @see TicketTable#of(String)
     */
    public static Table ticket(String ticket) {
        return of(TicketTable.of(ticket));
    }

    /**
     * Equivalent to {@code of(TicketTable.of(ticket))}.
     *
     * @param ticket the ticket
     * @return the ticket table
     * @see TicketTable#of(byte[])
     */
    public static Table ticket(byte[] ticket) {
        return of(TicketTable.of(ticket));
    }
}
