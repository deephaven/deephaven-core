//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.api.TableOperationsDefaults;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreator.OperationsToTable;
import io.deephaven.qst.TableCreator.TableToOperations;
import org.immutables.value.Value.Derived;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * A table specification is a declarative description of a table query. Part of a "query syntax tree".
 *
 * <p>
 * A table specification may be built-up explicitly via the individual implementation class build patterns, or may be
 * built-up in a fluent-manner via the {@link TableOperations} interface.
 *
 * <p>
 * A table specification can be "replayed" against the fluent interfaces, see
 * {@link TableCreator#create(TableCreator, TableToOperations, OperationsToTable, TableSpec)}.
 *
 * @see TableCreator
 * @see io.deephaven.api.TableOperations
 */
public interface TableSpec extends TableOperationsDefaults<TableSpec, TableSpec>, TableSchema {

    static EmptyTable empty(long size) {
        return EmptyTable.of(size);
    }

    static MergeTable merge(TableSpec first, TableSpec second, TableSpec... rest) {
        return MergeTable.builder().addTables(first, second).addTables(rest).build();
    }

    static TableSpec merge(Collection<? extends TableSpec> tables) {
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("Can't merge an empty collection");
        }
        if (tables.size() == 1) {
            return tables.iterator().next();
        }
        return MergeTable.of(tables);
    }

    static TableSpec of(TableCreationLogic logic) {
        return logic.create(TableCreatorImpl.INSTANCE);
    }

    /**
     * Create a ticket table with the UTF-8 bytes from the {@code ticket} string.
     *
     * @param ticket the ticket
     * @return the ticket table
     * @deprecated prefer {@link #ticket(byte[])}
     */
    @Deprecated
    static TicketTable ticket(String ticket) {
        return TicketTable.of(ticket.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Create a ticket table with the {@code ticket} bytes.
     *
     * @param ticket the ticket
     * @return the ticket table
     */
    static TicketTable ticket(byte[] ticket) {
        return TicketTable.of(ticket);
    }

    TableCreationLogic logic();

    /**
     * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
     * depth of zero.
     *
     * @return the depth
     */
    @Derived
    default int depth() {
        return ParentsVisitor.getParents(this).mapToInt(TableSpec::depth).max().orElse(-1) + 1;
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(EmptyTable emptyTable);

        T visit(NewTable newTable);

        T visit(TimeTable timeTable);

        T visit(MergeTable mergeTable);

        T visit(HeadTable headTable);

        T visit(TailTable tailTable);

        T visit(SliceTable sliceTable);

        T visit(ReverseTable reverseTable);

        T visit(SortTable sortTable);

        T visit(SnapshotTable snapshotTable);

        T visit(SnapshotWhenTable snapshotWhenTable);

        T visit(WhereTable whereTable);

        T visit(WhereInTable whereInTable);

        T visit(NaturalJoinTable naturalJoinTable);

        T visit(ExactJoinTable exactJoinTable);

        T visit(JoinTable joinTable);

        T visit(AsOfJoinTable aj);

        T visit(RangeJoinTable rangeJoinTable);

        T visit(ViewTable viewTable);

        T visit(SelectTable selectTable);

        T visit(UpdateViewTable updateViewTable);

        T visit(UpdateTable updateTable);

        T visit(LazyUpdateTable lazyUpdateTable);

        T visit(AggregateTable aggregateTable);

        T visit(AggregateAllTable aggregateAllTable);

        T visit(TicketTable ticketTable);

        T visit(InputTable inputTable);

        T visit(SelectDistinctTable selectDistinctTable);

        T visit(UpdateByTable updateByTable);

        T visit(UngroupTable ungroupTable);

        T visit(DropColumnsTable dropColumnsTable);

        T visit(MultiJoinTable multiJoinTable);
    }
}
