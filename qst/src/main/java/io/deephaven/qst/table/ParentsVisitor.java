//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.qst.table.TableSpec.Visitor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A visitor that returns the parent tables (if any) of the given table.
 */
public enum ParentsVisitor implements Visitor<Stream<TableSpec>> {
    INSTANCE;

    /**
     * A traversal of the table's parents. Does not perform de-duplication.
     *
     * @param table the table
     * @return the parents stream
     */
    public static Stream<TableSpec> getParents(TableSpec table) {
        return table.walk(INSTANCE);
    }

    /**
     * Create a post-order set from {@code tables}.
     *
     * <p>
     * Post-order means that for any given table, the table's dependencies will come before the table itself. There may
     * be multiple valid post-orderings; callers should not rely on a specific post-ordering.
     *
     * @param tables the tables
     * @return the post-order set
     */
    public static Set<TableSpec> postOrder(Iterable<TableSpec> tables) {
        // Note: we *can't* use TreeSet and anyOrder here because the comparator is an an auxiliary
        // attribute of TableSpec, and doesn't mesh with equals(). And even if we could, we might
        // not want to since it's an iterative approach to sorting instead of an upfront approach.
        return new LinkedHashSet<>(postOrderList(tables));
    }

    /**
     * Create a de-duplicated, post-order list from {@code tables}.
     *
     * <p>
     * Post-order means that for any given table, the table's dependencies will come before the table itself. There may
     * be multiple valid post-orderings; callers should not rely on a specific post-ordering.
     *
     * @param tables the tables
     * @return the de-duplicated, post-order list
     */
    public static List<TableSpec> postOrderList(Iterable<TableSpec> tables) {
        List<TableSpec> postOrder = new ArrayList<>(reachable(tables));
        postOrder.sort(Comparator.comparingInt(TableSpec::depth));
        return postOrder;
    }

    /**
     * Invoke the {@code consumer} for each table in the de-duplicated, post-order walk from {@code tables}.
     *
     * <p>
     * Post-order means that for any given table, the table's dependencies will come before the table itself. There may
     * be multiple valid post-orderings; callers should not rely on a specific post-ordering.
     *
     * @param tables the tables
     * @param consumer the consumer
     */
    public static void postOrderWalk(Iterable<TableSpec> tables, Consumer<TableSpec> consumer) {
        postOrderList(tables).forEach(consumer);
    }

    /**
     * Walk the {@code visitor} for each table in the de-duplicated, post-order walk from {@code tables}.
     *
     * <p>
     * Post-order means that for any given table, the table's dependencies will come before the table itself. There may
     * be multiple valid post-orderings; callers should not rely on a specific post-ordering.
     *
     * @param tables the tables
     * @param visitor the visitor
     */
    public static void postOrderWalk(Iterable<TableSpec> tables, Visitor visitor) {
        postOrderList(tables).forEach(t -> t.walk(visitor));
    }

    /**
     * Create a reachable set from {@code tables}, including {@code tables}. May be in any order.
     *
     * @param tables the tables
     * @return the reachable set
     */
    public static Set<TableSpec> reachable(Iterable<TableSpec> tables) {
        final Search search = new Search(null, null);
        return search.reachable(tables);
    }

    /**
     * Performs a search for a table that satisfies {@code searchPredicate}. Will follow the dependencies of
     * {@code initialInputs}. Tables that match {@code excludePaths} will not be returned, and will not have its
     * dependencies added to the search.
     *
     * <p>
     * Note: a dependency of a table that matches {@code excludePaths} will be returned if there is any path to that
     * dependency that doesn't go through {@code excludePaths}.
     */
    public static Optional<TableSpec> search(Iterable<TableSpec> initialInputs,
            Predicate<TableSpec> excludePaths, Predicate<TableSpec> searchPredicate) {
        final Search search = new Search(excludePaths, searchPredicate);
        return search.search(initialInputs);
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
    public Stream<TableSpec> visit(EmptyTable emptyTable) {
        return none();
    }

    @Override
    public Stream<TableSpec> visit(NewTable newTable) {
        return none();
    }

    @Override
    public Stream<TableSpec> visit(TimeTable timeTable) {
        return none();
    }

    @Override
    public Stream<TableSpec> visit(MergeTable mergeTable) {
        return mergeTable.tables().stream();
    }

    @Override
    public Stream<TableSpec> visit(HeadTable headTable) {
        return single(headTable);
    }

    @Override
    public Stream<TableSpec> visit(TailTable tailTable) {
        return single(tailTable);
    }

    @Override
    public Stream<TableSpec> visit(SliceTable sliceTable) {
        return single(sliceTable);
    }

    @Override
    public Stream<TableSpec> visit(ReverseTable reverseTable) {
        return single(reverseTable);
    }

    @Override
    public Stream<TableSpec> visit(SortTable sortTable) {
        return single(sortTable);
    }

    @Override
    public Stream<TableSpec> visit(SnapshotTable snapshotTable) {
        return single(snapshotTable);
    }

    @Override
    public Stream<TableSpec> visit(SnapshotWhenTable snapshotWhenTable) {
        return Stream.of(snapshotWhenTable.base(), snapshotWhenTable.trigger());
    }

    @Override
    public Stream<TableSpec> visit(WhereTable whereTable) {
        return single(whereTable);
    }

    @Override
    public Stream<TableSpec> visit(WhereInTable whereInTable) {
        return Stream.of(whereInTable.left(), whereInTable.right());
    }

    @Override
    public Stream<TableSpec> visit(NaturalJoinTable naturalJoinTable) {
        return Stream.of(naturalJoinTable.left(), naturalJoinTable.right());
    }

    @Override
    public Stream<TableSpec> visit(ExactJoinTable exactJoinTable) {
        return Stream.of(exactJoinTable.left(), exactJoinTable.right());
    }

    @Override
    public Stream<TableSpec> visit(JoinTable joinTable) {
        return Stream.of(joinTable.left(), joinTable.right());
    }

    @Override
    public Stream<TableSpec> visit(AsOfJoinTable aj) {
        return Stream.of(aj.left(), aj.right());
    }

    @Override
    public Stream<TableSpec> visit(RangeJoinTable rangeJoinTable) {
        return Stream.of(rangeJoinTable.left(), rangeJoinTable.right());
    }

    @Override
    public Stream<TableSpec> visit(ViewTable viewTable) {
        return single(viewTable);
    }

    @Override
    public Stream<TableSpec> visit(SelectTable selectTable) {
        return single(selectTable);
    }

    @Override
    public Stream<TableSpec> visit(UpdateViewTable updateViewTable) {
        return single(updateViewTable);
    }

    @Override
    public Stream<TableSpec> visit(UpdateTable updateTable) {
        return single(updateTable);
    }

    @Override
    public Stream<TableSpec> visit(LazyUpdateTable lazyUpdateTable) {
        return single(lazyUpdateTable);
    }

    @Override
    public Stream<TableSpec> visit(AggregateAllTable aggregateAllTable) {
        return single(aggregateAllTable);
    }

    @Override
    public Stream<TableSpec> visit(AggregateTable aggregateTable) {
        if (aggregateTable.initialGroups().isPresent()) {
            return Stream.of(aggregateTable.initialGroups().get(), aggregateTable.parent());
        } else {
            return Stream.of(aggregateTable.parent());
        }
    }

    @Override
    public Stream<TableSpec> visit(TicketTable ticketTable) {
        return none();
    }

    @Override
    public Stream<TableSpec> visit(InputTable inputTable) {
        return inputTable.schema().walk(new TableSchema.Visitor<Stream<TableSpec>>() {
            @Override
            public Stream<TableSpec> visit(TableSpec spec) {
                return Stream.of(spec);
            }

            @Override
            public Stream<TableSpec> visit(TableHeader header) {
                return none();
            }
        });
    }

    @Override
    public Stream<TableSpec> visit(SelectDistinctTable selectDistinctTable) {
        return single(selectDistinctTable);
    }

    @Override
    public Stream<TableSpec> visit(UpdateByTable updateByTable) {
        return single(updateByTable);
    }

    @Override
    public Stream<TableSpec> visit(UngroupTable ungroupTable) {
        return single(ungroupTable);
    }

    @Override
    public Stream<TableSpec> visit(DropColumnsTable dropColumnsTable) {
        return single(dropColumnsTable);
    }

    @Override
    public Stream<TableSpec> visit(MultiJoinTable multiJoinTable) {
        return multiJoinTable.inputs().stream().map(MultiJoinInput::table);
    }

    private static class Search {

        private final Predicate<TableSpec> excludePaths;
        private final Predicate<TableSpec> searchPredicate;
        private final Queue<TableSpec> toSearch = new ArrayDeque<>();

        // Note: this implementation has specifically been changed to give io.deephaven.sql.SqlAdapterTest temporary
        // stability. When we have a proper serialization format for TableSpec, we should opt to change this back to
        // a HashSet.
        private final Set<TableSpec> visited = new LinkedHashSet<>();

        private Search(Predicate<TableSpec> excludePaths, Predicate<TableSpec> searchPredicate) {
            this.excludePaths = excludePaths;
            this.searchPredicate = searchPredicate;
        }

        public Set<TableSpec> reachable(Iterable<TableSpec> initialInputs) {
            search(initialInputs);
            return visited;
        }

        public Optional<TableSpec> search(Iterable<TableSpec> initialInputs) {
            for (TableSpec initialInput : initialInputs) {
                toSearch.add(initialInput);
                do {
                    final TableSpec table = toSearch.remove();
                    if ((excludePaths == null || !excludePaths.test(table)) && visited.add(table)) {
                        if (searchPredicate != null && searchPredicate.test(table)) {
                            return Optional.of(table);
                        }
                        ParentsVisitor.getParents(table).forEachOrdered(toSearch::add);
                    }
                } while (!toSearch.isEmpty());
            }
            return Optional.empty();
        }
    }
}
