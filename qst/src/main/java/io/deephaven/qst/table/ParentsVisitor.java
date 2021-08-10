package io.deephaven.qst.table;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A visitor that returns the parent tables (if any) of the given table.
 */
public class ParentsVisitor implements TableSpec.Visitor {

    /**
     * A traversal of the table's parents. Does not perform de-duplication.
     *
     * @param table the table
     * @return the parents stream
     */
    public static Stream<TableSpec> getParents(TableSpec table) {
        return table.walk(new ParentsVisitor()).getOut();
    }

    /**
     * Create a post-order set from {@code tables}.
     *
     * <p>
     * Post-order means that for any given table, the table's dependencies will come before the
     * table itself. There may be multiple valid post-orderings; callers should not rely on a
     * specific post-ordering.
     *
     * @param tables the tables
     * @return the post-order set
     */
    public static Set<TableSpec> postOrder(Iterable<TableSpec> tables) {
        return postOrderImpl(tables);
    }

    /**
     * Note: this implementation suffers when the full
     */
    private static Set<TableSpec> postOrderImpl(Iterable<TableSpec> initialInputs) {

        // Tables that have been visited, post order
        final Set<TableSpec> postOrderVisited = new LinkedHashSet<>();

        // Tables that have outstanding dependencies
        final Set<TableSpec> preOrderStack = new LinkedHashSet<>();

        // Tables that we'll try to visit
        final Set<TableSpec> inputQueue = new LinkedHashSet<>();

        for (TableSpec initialInput : initialInputs) {
            if (postOrderVisited.contains(initialInput) || preOrderStack.contains(initialInput)) {
                continue;
            }
            inputQueue.add(initialInput);

            while (!inputQueue.isEmpty()) {
                final TableSpec table = removeFirst(inputQueue);
                final Iterator<TableSpec> it = getParents(table).iterator();
                boolean hasRemainingDependencies = false;
                while (it.hasNext()) {
                    final TableSpec dependency = it.next();
                    if (!postOrderVisited.contains(dependency)) {
                        inputQueue.add(dependency);
                        hasRemainingDependencies = true;
                    }
                }
                if (hasRemainingDependencies) {
                    // move table to the end of the stack
                    preOrderStack.remove(table);
                    preOrderStack.add(table);
                } else {
                    // table has no remaining dependencies, we can visit it!
                    postOrderVisited.add(table);
                    preOrderStack.remove(table);
                }
            }
        }

        // stack push was in pre-order
        // stack pop will be in post-order
        final List<TableSpec> reversed = new ArrayList<>(preOrderStack);
        Collections.reverse(reversed);
        postOrderVisited.addAll(reversed);

        return postOrderVisited;
    }

    private static <T> T removeFirst(Iterable<T> iterable) {
        final Iterator<T> it = iterable.iterator();
        final T item = it.next();
        it.remove();
        return item;
    }

    private Stream<TableSpec> out;

    private ParentsVisitor() {}

    public Stream<TableSpec> getOut() {
        return Objects.requireNonNull(out);
    }

    private static Stream<TableSpec> single(SingleParentTable singleParentTable) {
        return Stream.of(singleParentTable.parent());
    }

    private static Stream<TableSpec> none() {
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
    public void visit(SelectTable selectTable) {
        out = single(selectTable);
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
    public void visit(ByTable byTable) {
        out = single(byTable);
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        out = single(aggregationTable);
    }
}
