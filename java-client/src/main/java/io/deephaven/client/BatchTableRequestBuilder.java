package io.deephaven.client;

import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.Builder;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest.AggType;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest.Aggregate;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.backplane.grpc.JoinTablesRequest;
import io.deephaven.proto.backplane.grpc.JoinTablesRequest.Type;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.AggregationTable;
import io.deephaven.qst.table.ByTable;
import io.deephaven.api.ColumnAssignment;
import io.deephaven.api.ColumnFormula;
import io.deephaven.api.ColumnMatch;
import io.deephaven.api.ColumnName;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.api.Expression;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.QueryScopeTable;
import io.deephaven.api.RawString;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.api.Selectable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.Table;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereInTable;
import io.deephaven.qst.table.WhereNotInTable;
import io.deephaven.qst.table.WhereTable;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Array;
import io.deephaven.api.agg.Avg;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.CountDistinct;
import io.deephaven.api.agg.Distinct;
import io.deephaven.api.agg.First;
import io.deephaven.api.agg.Last;
import io.deephaven.api.agg.Max;
import io.deephaven.api.agg.Med;
import io.deephaven.api.agg.Min;
import io.deephaven.api.agg.Pct;
import io.deephaven.api.agg.Std;
import io.deephaven.api.agg.Sum;
import io.deephaven.api.agg.Var;
import io.deephaven.api.agg.WAvg;
import io.deephaven.api.agg.WSum;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

class BatchTableRequestBuilder {

    private final Set<Table> exports;
    private final Map<Table, Integer> indices;

    public BatchTableRequestBuilder() {
        exports = new HashSet<>();
        indices = new HashMap<>();
    }

    public synchronized BatchTableRequestBuilder addExports(Table... tables) {
        return addExports(Arrays.asList(tables));
    }

    public synchronized BatchTableRequestBuilder addExports(Collection<Table> tables) {
        exports.addAll(tables);
        return this;
    }

    public synchronized BatchTableRequest build() {
        final BatchTableRequest.Builder builder = BatchTableRequest.newBuilder();
        try (final Stream<Table> stream =
            exports.stream().flatMap(ParentsVisitor::getAncestorsAndSelf).distinct()) {
            final Iterator<Table> it = stream.iterator();
            for (int ix = 0; it.hasNext(); ix++) {
                final Table next = it.next();
                final boolean exported = exports.contains(next);
                final Ticket ticket = exported ? createExportTicket() : Ticket.getDefaultInstance();
                final Operation operation = next.walk(new OperationAdapter(ticket)).getOut();
                builder.addOps(operation);
                indices.put(next, ix);
            }
        }
        return builder.build();
    }

    private Ticket createExportTicket() {
        throw new UnsupportedOperationException("TODO");
    }

    private static <T> Operation op(BiFunction<Builder, T, Builder> f, T value) {
        return f.apply(Operation.newBuilder(), value).build();
    }

    private TableReference ref(Table table) {
        final Integer ix = indices.get(table);
        if (ix == null) {
            throw new IllegalStateException();
        }
        return TableReference.newBuilder().setBatchOffset(ix).build();
    }

    private class OperationAdapter implements Table.Visitor {
        private final Ticket ticket;
        private Operation out;

        OperationAdapter(Ticket ticket) {
            this.ticket = Objects.requireNonNull(ticket);
        }

        public Operation getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(EmptyTable emptyTable) {
            out = op(Builder::setEmptyTable,
                EmptyTableRequest.newBuilder().setResultId(ticket).setSize(emptyTable.size()));
        }

        @Override
        public void visit(NewTable newTable) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void visit(QueryScopeTable queryScopeTable) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void visit(HeadTable headTable) {
            out = op(Builder::setHead,
                HeadOrTailRequest.newBuilder().setResultId(ticket).setNumRows(headTable.size()));
        }

        @Override
        public void visit(TailTable tailTable) {
            out = op(Builder::setTail,
                HeadOrTailRequest.newBuilder().setResultId(ticket).setNumRows(tailTable.size()));
        }

        @Override
        public void visit(WhereTable whereTable) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void visit(WhereInTable whereInTable) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void visit(WhereNotInTable whereNotInTable) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void visit(NaturalJoinTable j) {
            out = join(Type.NATURAL_JOIN, j.left(), j.right(), j.matches(), j.additions());
        }

        @Override
        public void visit(ExactJoinTable j) {
            out = join(Type.EXACT_JOIN, j.left(), j.right(), j.matches(), j.additions());
        }

        @Override
        public void visit(JoinTable j) {
            out = join(Type.CROSS_JOIN, j.left(), j.right(), j.matches(), j.additions());
        }

        @Override
        public void visit(ViewTable v) {
            out = op(Builder::setView, selectOrUpdate(v, v.columns()));
        }

        @Override
        public void visit(UpdateViewTable v) {
            out = op(Builder::setUpdateView, selectOrUpdate(v, v.columns()));
        }

        @Override
        public void visit(UpdateTable v) {
            out = op(Builder::setUpdate, selectOrUpdate(v, v.columns()));
        }

        @Override
        public void visit(SelectTable v) {
            out = op(Builder::setSelect, selectOrUpdate(v, v.columns()));
        }

        @Override
        public void visit(ByTable byTable) {
            ComboAggregateRequest.Builder builder =
                ComboAggregateRequest.newBuilder().setResultId(ticket);
            for (Selectable column : byTable.columns()) {
                builder = builder.addGroupByColumns(SelectableString.of(column));
            }
            out = op(Builder::setComboAggregate, builder);
        }

        @Override
        public void visit(AggregationTable aggregationTable) {
            ComboAggregateRequest.Builder builder =
                ComboAggregateRequest.newBuilder().setResultId(ticket);
            for (Selectable column : aggregationTable.columns()) {
                builder = builder.addGroupByColumns(SelectableString.of(column));
            }
            for (Aggregation aggregation : aggregationTable.aggregations()) {
                builder = builder.addAggregates(AggregationAdapter.of(aggregation));
            }
            out = op(Builder::setComboAggregate, builder);
        }

        private Operation join(Type type, Table left, Table right, Collection<JoinMatch> matches,
            Collection<JoinAddition> additions) {
            JoinTablesRequest.Builder joinBuilder = JoinTablesRequest.newBuilder()
                .setResultId(ticket).setJoinType(type).setLeftId(ref(left)).setRightId(ref(right));
            for (JoinMatch match : matches) {
                joinBuilder = joinBuilder.addColumnsToMatch(JoinMatchString.of(match));
            }
            for (JoinAddition addition : additions) {
                joinBuilder = joinBuilder.addColumnsToAdd(JoinAdditionString.of(addition));
            }
            return op(Builder::setJoin, joinBuilder);
        }

        private SelectOrUpdateRequest selectOrUpdate(SingleParentTable x,
            Collection<Selectable> columns) {
            SelectOrUpdateRequest.Builder builder =
                SelectOrUpdateRequest.newBuilder().setResultId(ticket).setSourceId(ref(x.parent()));
            for (Selectable column : columns) {
                builder = builder.addColumnSpecs(SelectableString.of(column));
            }
            return builder.build();
        }
    }

    private static class JoinMatchString implements JoinMatch.Visitor {

        public static String of(JoinMatch match) {
            return match.walk(new JoinMatchString()).getOut();
        }

        private String out;

        public String getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName columnName) {
            out = columnName.name();
        }

        @Override
        public void visit(ColumnMatch columnMatch) {
            out = String.format("%s=%s", columnMatch.left(), columnMatch.right());
        }
    }

    private static class JoinAdditionString implements JoinAddition.Visitor {
        public static String of(JoinAddition addition) {
            return addition.walk(new JoinAdditionString()).getOut();
        }

        private String out;

        public String getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName columnName) {
            out = columnName.name();
        }

        @Override
        public void visit(ColumnAssignment columnAssignment) {
            out = String.format("%s=%s", columnAssignment.newColumn(),
                columnAssignment.existingColumn());
        }
    }

    private static class SelectableString implements Selectable.Visitor {
        public static String of(Selectable selectable) {
            return selectable.walk(new SelectableString()).getOut();
        }

        private String out;

        public String getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName columnName) {
            out = columnName.name();
        }

        @Override
        public void visit(ColumnFormula columnFormula) {
            out = String.format("%s=%s", columnFormula.newColumn().name(),
                ExpressionString.of(columnFormula.expression()));
        }
    }

    private static class ExpressionString implements Expression.Visitor {
        public static String of(Expression expression) {
            return expression.walk(new ExpressionString()).getOut();
        }

        private String out;

        public String getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName name) {
            out = name.name();
        }

        @Override
        public void visit(RawString rawString) {
            out = rawString.value();
        }
    }

    private static class AggregationAdapter implements Aggregation.Visitor {

        public static Aggregate of(Aggregation aggregation) {
            return aggregation.walk(new AggregationAdapter()).getOut();
        }

        private Aggregate out;

        public Aggregate getOut() {
            return Objects.requireNonNull(out);
        }

        private Aggregate.Builder of(AggType type, Aggregation agg) {
            return Aggregate.newBuilder().setType(type)
                .addMatchPairs(JoinMatchString.of(agg.match()));
        }

        @Override
        public void visit(Min min) {
            out = of(AggType.MIN, min).build();
        }

        @Override
        public void visit(Max max) {
            out = of(AggType.MAX, max).build();

        }

        @Override
        public void visit(Sum sum) {
            out = of(AggType.SUM, sum).build();
        }

        @Override
        public void visit(Var var) {
            out = of(AggType.VAR, var).build();
        }

        @Override
        public void visit(Avg avg) {
            out = of(AggType.AVG, avg).build();
        }

        @Override
        public void visit(First first) {
            out = of(AggType.FIRST, first).build();
        }

        @Override
        public void visit(Last last) {
            out = of(AggType.LAST, last).build();
        }

        @Override
        public void visit(Std std) {
            out = of(AggType.STD, std).build();
        }

        @Override
        public void visit(Med med) {
            if (!med.averageMedian()) {
                throw new UnsupportedOperationException("TODO: need to plumb through");
            }
            out = of(AggType.MEDIAN, med).build();
        }

        @Override
        public void visit(Pct pct) {
            if (pct.averageMedian()) {
                throw new UnsupportedOperationException("TODO: need to plumb through");
            }
            out = of(AggType.PERCENTILE, pct).build();
        }

        @Override
        public void visit(WSum wSum) {
            throw new UnsupportedOperationException("TODO: need to plumb through");
        }

        @Override
        public void visit(WAvg wAvg) {
            out = of(AggType.WEIGHTED_AVG, wAvg).setColumnName(wAvg.weight().name()).build();
        }

        @Override
        public void visit(Count count) {
            out = of(AggType.COUNT, count).build();
        }

        @Override
        public void visit(CountDistinct countDistinct) {
            throw new UnsupportedOperationException("TODO: need to plumb through");
        }

        @Override
        public void visit(Distinct distinct) {
            throw new UnsupportedOperationException("TODO: need to plumb through");
        }

        @Override
        public void visit(Array array) {
            out = of(AggType.ARRAY, array).build();
        }
    }
}
