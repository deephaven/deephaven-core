package io.deephaven.qst.table;

import java.util.Objects;
import java.util.stream.Stream;

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
     * A depth-first traversal of the table's ancestors, and the table. Does not perform
     * de-duplication.
     *
     * @param table the table
     * @return the ancestors and table stream
     */
    public static Stream<Table> getAncestorsAndSelf(Table table) {
        return Stream.concat(getAncestorsDepthFirst(table), Stream.of(table));
    }

    /**
     * An in-order traversal of the table and the table's ancestors. Does not perform
     * de-duplication.
     *
     * @param table the table
     * @return the table and ancestors stream
     */
    public static Stream<Table> getSelfAndAncestors(Table table) {
        return Stream.concat(Stream.of(table), getAncestorsInOrder(table));
    }

    private static Stream<Table> getAncestorsDepthFirst(Table table) {
        return getParents(table).flatMap(ParentsVisitor::getAncestorsAndSelf);
    }

    private static Stream<Table> getAncestorsInOrder(Table table) {
        return getParents(table).flatMap(ParentsVisitor::getSelfAndAncestors);
    }

    private Stream<Table> out;

    private ParentsVisitor() {}

    public Stream<Table> getOut() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(EmptyTable emptyTable) {
        out = Stream.empty();
    }

    @Override
    public void visit(NewTable newTable) {
        out = Stream.empty();
    }

    @Override
    public void visit(QueryScopeTable queryScopeTable) {
        out = Stream.empty();
    }

    @Override
    public void visit(HeadTable headTable) {
        out = Stream.of(headTable.parent());
    }

    @Override
    public void visit(TailTable tailTable) {
        out = Stream.of(tailTable.parent());
    }

    @Override
    public void visit(ReverseTable reverseTable) {
        out = Stream.of(reverseTable.parent());
    }

    @Override
    public void visit(SortTable sortTable) {
        out = Stream.of(sortTable.parent());
    }

    @Override
    public void visit(WhereTable whereTable) {
        out = Stream.of(whereTable.parent());
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
    public void visit(ViewTable viewTable) {
        out = Stream.of(viewTable.parent());
    }

    @Override
    public void visit(UpdateViewTable updateViewTable) {
        out = Stream.of(updateViewTable.parent());
    }

    @Override
    public void visit(UpdateTable updateTable) {
        out = Stream.of(updateTable.parent());
    }

    @Override
    public void visit(SelectTable selectTable) {
        out = Stream.of(selectTable.parent());
    }

    @Override
    public void visit(ByTable byTable) {
        out = Stream.of(byTable.parent());
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        out = Stream.of(aggregationTable.parent());
    }
}
