package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

public interface Table extends TableOperations<Table, Table>, Serializable {

    static EmptyTable empty(long size) {
        return EmptyTable.of(size);
    }

    static Table merge(Table... tables) {
        return merge(Arrays.asList(tables));
    }

    static Table merge(Collection<? extends Table> tables) {
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("Can't merge an empty collection");
        }
        if (tables.size() == 1) {
            return tables.iterator().next();
        }
        return MergeTable.of(tables);
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
    static Table file(Path path) throws IOException, ClassNotFoundException {
        try (InputStream in = Files.newInputStream(path);
            BufferedInputStream buf = new BufferedInputStream(in);
            ObjectInputStream oIn = new ObjectInputStream(buf)) {
            return (Table) oIn.readObject();
        }
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

        void visit(WhereTable whereTable);

        void visit(WhereInTable whereInTable);

        void visit(WhereNotInTable whereNotInTable);

        void visit(NaturalJoinTable naturalJoinTable);

        void visit(ExactJoinTable exactJoinTable);

        void visit(JoinTable joinTable);

        void visit(LeftJoinTable leftJoinTable);

        void visit(AsOfJoinTable aj);

        void visit(ReverseAsOfJoinTable raj);

        void visit(ViewTable viewTable);

        void visit(SelectTable selectTable);

        void visit(UpdateViewTable updateViewTable);

        void visit(UpdateTable updateTable);

        void visit(ByTable byTable);

        void visit(AggregationTable aggregationTable);
    }
}
