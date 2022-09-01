/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.qst.table.TableSpec.Visitor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
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
public class ParentsVisitor implements Visitor {

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
    public void visit(LazyUpdateTable lazyUpdateTable) {
        out = single(lazyUpdateTable);
    }

    @Override
    public void visit(AggregateAllByTable aggAllByTable) {
        out = single(aggAllByTable);
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        if (aggregationTable.initialGroups().isPresent()) {
            out = Stream.of(aggregationTable.initialGroups().get(), aggregationTable.parent());
        } else {
            out = Stream.of(aggregationTable.parent());
        }
    }

    @Override
    public void visit(TicketTable ticketTable) {
        out = none();
    }

    @Override
    public void visit(InputTable inputTable) {
        inputTable.schema().walk(new TableSchema.Visitor() {
            @Override
            public void visit(TableSpec spec) {
                out = Stream.of(spec);
            }

            @Override
            public void visit(TableHeader header) {
                out = none();
            }
        });
    }

    @Override
    public void visit(SelectDistinctTable selectDistinctTable) {
        out = single(selectDistinctTable);
    }

    @Override
    public void visit(CountByTable countByTable) {
        out = single(countByTable);
    }

    @Override
    public void visit(UpdateByTable updateByTable) {
        out = single(updateByTable);
    }

    private static class Search {

        private final Predicate<TableSpec> excludePaths;
        private final Predicate<TableSpec> searchPredicate;
        private final Queue<TableSpec> toSearch = new ArrayDeque<>();
        private final Set<TableSpec> visited = new HashSet<>();

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
