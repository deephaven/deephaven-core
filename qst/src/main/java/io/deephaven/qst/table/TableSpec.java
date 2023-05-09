/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.api.TableOperationsDefaults;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreator.OperationsToTable;
import io.deephaven.qst.TableCreator.TableToOperations;
import org.immutables.value.Value.Derived;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
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
public interface TableSpec extends TableOperationsDefaults<TableSpec, TableSpec>, TableSchema, Serializable {

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

    static TicketTable ticket(String ticket) {
        return TicketTable.of(ticket);
    }

    static TicketTable ticket(byte[] ticket) {
        return TicketTable.of(ticket);
    }

    /**
     * Create a table via java deserialization.
     *
     * <p>
     * Note: stability of the format is not guaranteed.
     *
     * @param path the path to the file
     * @return the table
     * @throws IOException if an I/O error occurs
     * @throws ClassNotFoundException Class of a serialized object cannot be found.
     */
    static TableSpec file(Path path) throws IOException, ClassNotFoundException {
        try (InputStream in = Files.newInputStream(path);
                BufferedInputStream buf = new BufferedInputStream(in);
                ObjectInputStream oIn = new ObjectInputStream(buf)) {
            return (TableSpec) oIn.readObject();
        }
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

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(EmptyTable emptyTable);

        void visit(NewTable newTable);

        void visit(TimeTable timeTable);

        void visit(MergeTable mergeTable);

        void visit(HeadTable headTable);

        void visit(TailTable tailTable);

        void visit(ReverseTable reverseTable);

        void visit(SortTable sortTable);

        void visit(SnapshotTable snapshotTable);

        void visit(SnapshotWhenTable snapshotWhenTable);

        void visit(WhereTable whereTable);

        void visit(WhereInTable whereInTable);

        void visit(NaturalJoinTable naturalJoinTable);

        void visit(ExactJoinTable exactJoinTable);

        void visit(JoinTable joinTable);

        void visit(AsOfJoinTable aj);

        void visit(ReverseAsOfJoinTable raj);

        void visit(RangeJoinTable rangeJoinTable);

        void visit(ViewTable viewTable);

        void visit(SelectTable selectTable);

        void visit(UpdateViewTable updateViewTable);

        void visit(UpdateTable updateTable);

        void visit(LazyUpdateTable lazyUpdateTable);

        void visit(AggregateTable aggregateTable);

        void visit(AggregateAllTable aggregateAllTable);

        void visit(TicketTable ticketTable);

        void visit(InputTable inputTable);

        void visit(SelectDistinctTable selectDistinctTable);

        void visit(UpdateByTable updateByTable);

        void visit(UngroupTable ungroupTable);

        void visit(DropColumnsTable dropColumnsTable);
    }
}
