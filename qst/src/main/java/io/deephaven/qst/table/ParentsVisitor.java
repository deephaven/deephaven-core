package io.deephaven.qst.table;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A visitor that returns the parent tables (if any) of the given table.
 */
public class ParentsVisitor implements Table.Visitor {

    /**
     * A traversal of the table's parents. Does not perform de-duplication.
     *
     * @param table the table
     * @return the parents stream
     */
    public static Stream<Table> getParents(Table table) {
        return table.walk(new ParentsVisitor()).getOut();
    }

    /**
     * Create a depth-first ordered set.
     *
     * @param tables the tables
     * @return a depth-first ordered set
     *
     * @see #depthFirstWalk(Iterable, Consumer)
     */
    public static Set<Table> depthFirst(Iterable<Table> tables) {
        Set<Table> set = new LinkedHashSet<>();
        depthFirstWalk(tables, set::add);
        return set;
    }


    /**
     * Create a depth-first ordered set, up to a depth of {@code maxDepth}.
     *
     * @param tables the tables
     * @param maxDepth the maximum depth
     * @return a depth-first ordered set
     *
     * @see #depthFirstWalk(Iterable, Consumer, int)
     */
    public static Set<Table> depthFirst(Iterable<Table> tables, int maxDepth) {
        Set<Table> set = new LinkedHashSet<>();
        depthFirstWalk(tables, set::add, maxDepth);
        return set;
    }

    /**
     * Walk the {@link Table tables} depth first with de-duplication.
     *
     * @param tables the tables
     * @param consumer the consumer
     */
    public static void depthFirstWalk(Iterable<Table> tables, Consumer<Table> consumer) {
        depthFirstWalk(tables, consumer, Integer.MAX_VALUE);
    }

    /**
     * Walk the {@link Table tables} depth-first with de-duplication, up to a depth of
     * {@code maxDepth}.
     *
     * @param tables the tables
     * @param consumer the consumer
     * @param maxDepth the maximum depth
     */
    public static void depthFirstWalk(Iterable<Table> tables, Consumer<Table> consumer,
        int maxDepth) {
        Set<Table> visited = new HashSet<>();
        for (Table table : tables) {
            depthFirstTraversal(visited, table, consumer, maxDepth);
        }
    }

    private static void depthFirstTraversal(Set<Table> visited, Table table,
        Consumer<Table> consumer, int maxDepth) {
        // This method is much more efficient than trying to accomplish the same with
        // Stream#distinct, since we
        // can cut off the duplication at the highest-level table.
        if (maxDepth < 0 || !visited.add(table)) {
            return;
        }
        if (maxDepth > 0) {
            try (Stream<Table> stream = getParents(table)) {
                Iterator<Table> it = stream.iterator();
                while (it.hasNext()) {
                    depthFirstTraversal(visited, it.next(), consumer, maxDepth - 1);
                }
            }
        }
        consumer.accept(table);
    }

    private Stream<Table> out;

    private ParentsVisitor() {}

    public Stream<Table> getOut() {
        return Objects.requireNonNull(out);
    }

    private static Stream<Table> single(SingleParentTable singleParentTable) {
        return Stream.of(singleParentTable.parent());
    }

    private static Stream<Table> none() {
        return Stream.empty();
    }

    @Override
    public void visit(EmptyTable emptyTable) {
        out = none();
    }

    @Override
    public void visit(NewTable newTable) {
        out = none();
    }

    @Override
    public void visit(TimeTable timeTable) {
        out = none();
    }

    @Override
    public void visit(MergeTable mergeTable) {
        out = mergeTable.tables().stream();
    }

    @Override
    public void visit(HeadTable headTable) {
        out = single(headTable);
    }

    @Override
    public void visit(TailTable tailTable) {
        out = single(tailTable);
    }

    @Override
    public void visit(ReverseTable reverseTable) {
        out = single(reverseTable);
    }

    @Override
    public void visit(SortTable sortTable) {
        out = single(sortTable);
    }

    @Override
    public void visit(SnapshotTable snapshotTable) {
        out = Stream.of(snapshotTable.base(), snapshotTable.trigger());
    }

    @Override
    public void visit(WhereTable whereTable) {
        out = single(whereTable);
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        out = Stream.of(whereInTable.left(), whereInTable.right());
    }

    @Override
    public void visit(WhereNotInTable whereNotInTable) {
        out = Stream.of(whereNotInTable.left(), whereNotInTable.right());
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        out = Stream.of(naturalJoinTable.left(), naturalJoinTable.right());
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        out = Stream.of(exactJoinTable.left(), exactJoinTable.right());
    }

    @Override
    public void visit(JoinTable joinTable) {
        out = Stream.of(joinTable.left(), joinTable.right());
    }

    @Override
    public void visit(LeftJoinTable leftJoinTable) {
        out = Stream.of(leftJoinTable.left(), leftJoinTable.right());
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        out = Stream.of(aj.left(), aj.right());
    }

    @Override
    public void visit(ReverseAsOfJoinTable raj) {
        out = Stream.of(raj.left(), raj.right());
    }

    @Override
    public void visit(ViewTable viewTable) {
        out = single(viewTable);
    }

    @Override
    public void visit(UpdateViewTable updateViewTable) {
        out = single(updateViewTable);
    }

    @Override
    public void visit(UpdateTable updateTable) {
        out = single(updateTable);
    }

    @Override
    public void visit(SelectTable selectTable) {
        out = single(selectTable);
    }

    @Override
    public void visit(ByTable byTable) {
        out = single(byTable);
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        out = single(aggregationTable);
    }
}
