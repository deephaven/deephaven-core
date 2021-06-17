package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.api.SortColumn;
import io.deephaven.api.SortColumn.Order;
import io.deephaven.api.Strings;
import io.deephaven.client.impl.ExportManagerImpl.State;
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
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortDescriptor.SortDirection;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.AggregationTable;
import io.deephaven.qst.table.ByTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.QueryScopeTable;
import io.deephaven.qst.table.ReverseTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.api.Selectable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.SortTable;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class BatchTableRequestBuilder {

    private static final Collector<ExportManagerImpl.State, ?, Map<Table, ExportManagerImpl.State>> TABLE_TO_EXPORT_COLLECTOR =
        Collectors.toMap(State::table, Function.identity());

    static BatchTableRequest build(List<ExportManagerImpl.State> states) {
        if (states.isEmpty()) {
            throw new IllegalArgumentException();
        }

        final Map<Table, ExportManagerImpl.State> tableToExport =
            states.stream().collect(TABLE_TO_EXPORT_COLLECTOR);

        // this is a depth-first ordering without duplicates, ensuring we create the dependencies
        // in the preferred/resolvable order
        final List<Table> tables = states.stream().map(ExportManagerImpl.State::table)
            .flatMap(ParentsVisitor::getAncestorsAndSelf).distinct().collect(Collectors.toList());

        final Map<Table, Integer> indices = new HashMap<>(tables.size());
        final BatchTableRequest.Builder builder = BatchTableRequest.newBuilder();
        int ix = 0;
        for (Table next : tables) {
            final ExportManagerImpl.State state = tableToExport.get(next);

            final Ticket ticket = state == null ? Ticket.getDefaultInstance()
                : Ticket.newBuilder().setId(longToByteString(state.ticket())).build();

            final Operation operation = next.walk(new OperationAdapter(ticket, indices)).getOut();
            builder.addOps(operation);
            indices.put(next, ix++);
        }

        return builder.build();
    }

    private static <T> Operation op(BiFunction<Builder, T, Builder> f, T value) {
        return f.apply(Operation.newBuilder(), value).build();
    }

    private static class OperationAdapter implements Table.Visitor {
        private final Ticket ticket;
        private final Map<Table, Integer> indices;
        private Operation out;

        OperationAdapter(Ticket ticket, Map<Table, Integer> indices) {
            this.ticket = Objects.requireNonNull(ticket);
            this.indices = Objects.requireNonNull(indices);
        }

        public Operation getOut() {
            return Objects.requireNonNull(out);
        }

        private TableReference ref(Table table) {
            final Integer ix = indices.get(table);
            if (ix == null) {
                throw new IllegalStateException();
            }
            return TableReference.newBuilder().setBatchOffset(ix).build();
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
            out = op(Builder::setHead, HeadOrTailRequest.newBuilder().setResultId(ticket)
                .setSourceId(ref(headTable.parent())).setNumRows(headTable.size()));
        }

        @Override
        public void visit(TailTable tailTable) {
            out = op(Builder::setTail, HeadOrTailRequest.newBuilder().setResultId(ticket)
                .setSourceId(ref(tailTable.parent())).setNumRows(tailTable.size()));
        }

        @Override
        public void visit(ReverseTable reverseTable) {
            // a bit hacky at the proto level, but this is how to specify a reverse
            out = op(Builder::setSort,
                SortTableRequest.newBuilder().setResultId(ticket)
                    .setSourceId(ref(reverseTable.parent()))
                    .addSorts(
                        SortDescriptor.newBuilder().setDirection(SortDirection.REVERSE).build())
                    .build());
        }

        @Override
        public void visit(SortTable sortTable) {
            SortTableRequest.Builder builder = SortTableRequest.newBuilder().setResultId(ticket)
                .setSourceId(ref(sortTable.parent()));
            for (SortColumn column : sortTable.columns()) {
                SortDescriptor descriptor =
                    SortDescriptor.newBuilder().setColumnName(column.column().name())
                        .setDirection(column.order() == Order.ASCENDING ? SortDirection.ASCENDING
                            : SortDirection.DESCENDING)
                        .build();
                builder.addSorts(descriptor);
            }
            out = op(Builder::setSort, builder.build());
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
            ComboAggregateRequest.Builder builder = ComboAggregateRequest.newBuilder()
                .setResultId(ticket).setSourceId(ref(byTable.parent()));
            for (Selectable column : byTable.columns()) {
                builder.addGroupByColumns(Strings.of(column));
            }
            out = op(Builder::setComboAggregate, builder);
        }

        @Override
        public void visit(AggregationTable aggregationTable) {
            ComboAggregateRequest.Builder builder = ComboAggregateRequest.newBuilder()
                .setResultId(ticket).setSourceId(ref(aggregationTable.parent()));
            for (Selectable column : aggregationTable.columns()) {
                builder.addGroupByColumns(Strings.of(column));
            }
            for (Aggregation aggregation : aggregationTable.aggregations()) {
                builder.addAggregates(AggregationAdapter.of(aggregation));
            }
            out = op(Builder::setComboAggregate, builder);
        }

        private Operation join(Type type, Table left, Table right, Collection<JoinMatch> matches,
            Collection<JoinAddition> additions) {
            JoinTablesRequest.Builder builder = JoinTablesRequest.newBuilder().setResultId(ticket)
                .setJoinType(type).setLeftId(ref(left)).setRightId(ref(right));
            for (JoinMatch match : matches) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : additions) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            return op(Builder::setJoin, builder);
        }

        private SelectOrUpdateRequest selectOrUpdate(SingleParentTable x,
            Collection<Selectable> columns) {
            SelectOrUpdateRequest.Builder builder =
                SelectOrUpdateRequest.newBuilder().setResultId(ticket).setSourceId(ref(x.parent()));
            for (Selectable column : columns) {
                builder.addColumnSpecs(Strings.of(column));
            }
            return builder.build();
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
            return Aggregate.newBuilder().setType(type).addMatchPairs(Strings.of(agg.match()));
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

    public static ByteString longToByteString(long value) {
        // todo: make common location
        // Note: Little-Endian
        final byte[] result = new byte[8];
        for (int i = 0; i < 8; i++) {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
        return ByteString.copyFrom(result);
    }
}
