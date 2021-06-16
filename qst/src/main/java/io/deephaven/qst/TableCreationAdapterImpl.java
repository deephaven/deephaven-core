package io.deephaven.qst;

import io.deephaven.qst.table.AggregationTable;
import io.deephaven.qst.table.ByTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.QueryScopeTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.Table;
import io.deephaven.qst.table.Table.Visitor;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereInTable;
import io.deephaven.qst.table.WhereNotInTable;
import io.deephaven.qst.table.WhereTable;
import java.util.Objects;

class TableCreationAdapterImpl<BUILDER extends TableOperations<BUILDER, TABLE>, TABLE>
    implements Visitor {

    static <BUILDER extends TableOperations<BUILDER, TABLE>, TABLE> BUILDER of(
        TableCreation<BUILDER, TABLE> creation, Table table) {
        return table.walk(new TableCreationAdapterImpl<>(creation)).getOut();
    }

    private final TableCreation<BUILDER, TABLE> tableCreation;
    private BUILDER out;

    private TableCreationAdapterImpl(TableCreation<BUILDER, TABLE> tableCreation) {
        this.tableCreation = Objects.requireNonNull(tableCreation);
    }

    public BUILDER getOut() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(EmptyTable emptyTable) {
        out = tableCreation.of(emptyTable);
    }

    @Override
    public void visit(NewTable newTable) {
        out = tableCreation.of(newTable);
    }

    @Override
    public void visit(QueryScopeTable queryScopeTable) {
        out = tableCreation.of(queryScopeTable);
    }

    @Override
    public void visit(HeadTable headTable) {
        out = parent(headTable).head(headTable.size());
    }

    @Override
    public void visit(TailTable tailTable) {
        out = parent(tailTable).tail(tailTable.size());
    }

    @Override
    public void visit(WhereTable whereTable) {
        out = parent(whereTable).where2(whereTable.filters());
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        final BUILDER left = of(tableCreation, whereInTable.left());
        final BUILDER right = of(tableCreation, whereInTable.right());
        out = left.whereIn2(right.toTable(), whereInTable.matches());
    }

    @Override
    public void visit(WhereNotInTable whereNotInTable) {
        final BUILDER left = of(tableCreation, whereNotInTable.left());
        final BUILDER right = of(tableCreation, whereNotInTable.right());
        out = left.whereNotIn2(right.toTable(), whereNotInTable.matches());
    }

    @Override
    public void visit(ViewTable viewTable) {
        out = parent(viewTable).view2(viewTable.columns());
    }

    @Override
    public void visit(UpdateViewTable updateViewTable) {
        out = parent(updateViewTable).updateView2(updateViewTable.columns());
    }

    @Override
    public void visit(UpdateTable updateTable) {
        out = parent(updateTable).update2(updateTable.columns());
    }

    @Override
    public void visit(SelectTable selectTable) {
        // todo: consider breaking these types up?
        if (selectTable.isSelectAll()) {
            out = parent(selectTable).select();
        } else {
            out = parent(selectTable).select2(selectTable.columns());
        }
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        final BUILDER left = of(tableCreation, naturalJoinTable.left());
        final BUILDER right = of(tableCreation, naturalJoinTable.right());
        out = left.naturalJoin2(right.toTable(), naturalJoinTable.matches(),
            naturalJoinTable.additions());
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        final BUILDER left = of(tableCreation, exactJoinTable.left());
        final BUILDER right = of(tableCreation, exactJoinTable.right());
        out =
            left.exactJoin2(right.toTable(), exactJoinTable.matches(), exactJoinTable.additions());
    }

    @Override
    public void visit(JoinTable joinTable) {
        final BUILDER left = of(tableCreation, joinTable.left());
        final BUILDER right = of(tableCreation, joinTable.right());
        out = left.join2(right.toTable(), joinTable.matches(), joinTable.additions());
    }

    @Override
    public void visit(ByTable byTable) {
        out = parent(byTable).by2(byTable.columns());
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        out = parent(aggregationTable).by(aggregationTable.columns(),
            aggregationTable.aggregations());
    }

    private BUILDER parent(SingleParentTable table) {
        return of(tableCreation, table.parent());
    }
}
