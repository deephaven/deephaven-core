package io.deephaven.client.impl;

import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.ReverseAsOfJoinRule;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.SortColumn.Order;
import io.deephaven.api.Strings;
import io.deephaven.api.agg.AbsSum;
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
import io.deephaven.api.agg.Multi;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.agg.Pct;
import io.deephaven.api.agg.SortedFirst;
import io.deephaven.api.agg.SortedLast;
import io.deephaven.api.agg.Std;
import io.deephaven.api.agg.Sum;
import io.deephaven.api.agg.Unique;
import io.deephaven.api.agg.Var;
import io.deephaven.api.agg.WAvg;
import io.deephaven.api.agg.WSum;
import io.deephaven.api.filter.Filter;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.Builder;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest.AggType;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest.Aggregate;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.backplane.grpc.LeftJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortDescriptor.SortDirection;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.deephaven.qst.table.AggregationTable;
import io.deephaven.qst.table.AsOfJoinTable;
import io.deephaven.qst.table.ByTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.LeftJoinTable;
import io.deephaven.qst.table.MergeTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.ReverseAsOfJoinTable;
import io.deephaven.qst.table.ReverseTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.SnapshotTable;
import io.deephaven.qst.table.SortTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.TimeProvider.Visitor;
import io.deephaven.qst.table.TimeProviderSystem;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereInTable;
import io.deephaven.qst.table.WhereNotInTable;
import io.deephaven.qst.table.WhereTable;
import org.apache.arrow.flight.impl.Flight.Ticket;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

class BatchTableRequestBuilder {

    interface ExportLookup {
        Optional<Ticket> ticket(TableSpec spec);
    }

    /**
     * Creates an optimal batch table request for new exports, leveraging the existing exports to
     * reduce duplication
     */
    static BatchTableRequest build(ExportLookup lookup, Set<TableSpec> newExports) {
        if (newExports.isEmpty()) {
            throw new IllegalStateException(
                "Shouldn't be building a batch request if there aren't any new exports");
        }

        // this is a post-order without duplicates, ensuring we create the dependencies
        // in the preferred/resolvable order
        final Set<TableSpec> dependents = ParentsVisitor.postOrder(newExports);

        final Map<TableSpec, Integer> indices = new HashMap<>(dependents.size());
        final BatchTableRequest.Builder builder = BatchTableRequest.newBuilder();
        int ix = 0;
        for (TableSpec tableSpec : dependents) {
            final Optional<Ticket> t = lookup.ticket(tableSpec);
            final boolean isExport = t.isPresent();
            final Ticket ticket;
            if (isExport) {
                ticket = t.get();
                if (ticket.equals(Ticket.getDefaultInstance())) {
                    throw new IllegalStateException(
                        "Found an \"export\", but it is using the default empty ticket");
                }
                if (!newExports.contains(tableSpec)) {
                    // we've already exported it, no need to include it in our batch request
                    continue;
                }
                // it's a new export
            } else {
                if (newExports.contains(tableSpec)) {
                    throw new IllegalStateException(
                        "Found a \"new export\", but it was not present in ExportLookup");
                }
                // is not an export, so we need to provide an empty ticket
                ticket = Ticket.getDefaultInstance();
            }

            final Operation operation =
                tableSpec.walk(new OperationAdapter(ticket, indices, lookup)).getOut();
            builder.addOps(operation);
            indices.put(tableSpec, ix++);
        }

        return builder.build();
    }

    private static <T> Operation op(BiFunction<Builder, T, Builder> f, T value) {
        return f.apply(Operation.newBuilder(), value).build();
    }

    private static class OperationAdapter implements TableSpec.Visitor {
        private final Ticket ticket;
        private final Map<TableSpec, Integer> indices;
        private final ExportLookup lookup;
        private Operation out;

        OperationAdapter(Ticket ticket, Map<TableSpec, Integer> indices, ExportLookup lookup) {
            this.ticket = Objects.requireNonNull(ticket);
            this.indices = Objects.requireNonNull(indices);
            this.lookup = Objects.requireNonNull(lookup);
        }

        public Operation getOut() {
            return Objects.requireNonNull(out);
        }

        private TableReference ref(TableSpec table) {
            Optional<Ticket> existing = lookup.ticket(table);
            if (existing.isPresent()) {
                return TableReference.newBuilder().setTicket(existing.get()).build();
            }
            final Integer ix = indices.get(table);
            if (ix != null) {
                return TableReference.newBuilder().setBatchOffset(ix).build();
            }
            throw new IllegalStateException(
                "Unable to reference table - batch table request logic has a bug.");
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
        public void visit(TimeTable timeTable) {
            // noinspection Convert2Lambda
            timeTable.timeProvider().walk(new Visitor() {
                @Override
                public void visit(TimeProviderSystem system) {
                    // OK
                    // note: if other time providers are added, we need to throw an exception
                }
            });

            TimeTableRequest.Builder builder = TimeTableRequest.newBuilder().setResultId(ticket)
                .setPeriodNanos(timeTable.interval().toNanos());
            if (timeTable.startTime().isPresent()) {
                final Instant startTime = timeTable.startTime().get();
                final long epochNanos = Math.addExact(
                    TimeUnit.SECONDS.toNanos(startTime.getEpochSecond()), startTime.getNano());
                builder.setStartTimeNanos(epochNanos);
            }
            out = op(Builder::setTimeTable, builder.build());
        }

        @Override
        public void visit(MergeTable mergeTable) {
            MergeTablesRequest.Builder builder =
                MergeTablesRequest.newBuilder().setResultId(ticket);
            for (TableSpec table : mergeTable.tables()) {
                builder.addSourceIds(ref(table));
            }
            out = op(Builder::setMerge, builder.build());
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
        public void visit(SnapshotTable snapshotTable) {
            SnapshotTableRequest.Builder builder = SnapshotTableRequest.newBuilder()
                .setResultId(ticket).setDoInitialSnapshot(snapshotTable.doInitialSnapshot())
                .setLeftId(ref(snapshotTable.trigger())).setRightId(ref(snapshotTable.base()));
            for (ColumnName stampColumn : snapshotTable.stampColumns()) {
                builder.addStampColumns(stampColumn.name());
            }
            out = op(Builder::setSnapshot, builder.build());
        }

        @Override
        public void visit(WhereTable whereTable) {
            // todo: use structured one
            UnstructuredFilterTableRequest.Builder builder = UnstructuredFilterTableRequest
                .newBuilder().setResultId(ticket).setSourceId(ref(whereTable.parent()));
            for (Filter filter : whereTable.filters()) {
                builder.addFilters(Strings.of(filter));
            }
            out = op(Builder::setUnstructuredFilter, builder.build());
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
            NaturalJoinTablesRequest.Builder builder = NaturalJoinTablesRequest.newBuilder()
                .setResultId(ticket).setLeftId(ref(j.left())).setRightId(ref(j.right()));
            for (JoinMatch match : j.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : j.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            out = op(Builder::setNaturalJoin, builder.build());
        }

        @Override
        public void visit(ExactJoinTable j) {
            ExactJoinTablesRequest.Builder builder = ExactJoinTablesRequest.newBuilder()
                .setResultId(ticket).setLeftId(ref(j.left())).setRightId(ref(j.right()));
            for (JoinMatch match : j.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : j.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            out = op(Builder::setExactJoin, builder.build());
        }

        @Override
        public void visit(JoinTable j) {
            CrossJoinTablesRequest.Builder builder =
                CrossJoinTablesRequest.newBuilder().setResultId(ticket).setLeftId(ref(j.left()))
                    .setRightId(ref(j.right())).setReserveBits(j.reserveBits());
            for (JoinMatch match : j.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : j.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            out = op(Builder::setCrossJoin, builder.build());
        }

        @Override
        public void visit(LeftJoinTable j) {
            LeftJoinTablesRequest.Builder builder = LeftJoinTablesRequest.newBuilder()
                .setResultId(ticket).setLeftId(ref(j.left())).setRightId(ref(j.right()));
            for (JoinMatch match : j.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : j.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            out = op(Builder::setLeftJoin, builder.build());
        }

        @Override
        public void visit(AsOfJoinTable aj) {
            AsOfJoinTablesRequest.Builder builder = AsOfJoinTablesRequest.newBuilder()
                .setLeftId(ref(aj.left())).setRightId(ref(aj.right()))
                .setAsOfMatchRule(aj.rule() == AsOfJoinRule.LESS_THAN_EQUAL
                    ? AsOfJoinTablesRequest.MatchRule.LESS_THAN_EQUAL
                    : AsOfJoinTablesRequest.MatchRule.LESS_THAN);
            for (JoinMatch match : aj.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : aj.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            out = op(Builder::setAsOfJoin, builder.build());
        }

        @Override
        public void visit(ReverseAsOfJoinTable raj) {
            AsOfJoinTablesRequest.Builder builder = AsOfJoinTablesRequest.newBuilder()
                .setLeftId(ref(raj.left())).setRightId(ref(raj.right()))
                .setAsOfMatchRule(raj.rule() == ReverseAsOfJoinRule.GREATER_THAN_EQUAL
                    ? AsOfJoinTablesRequest.MatchRule.GREATER_THAN_EQUAL
                    : AsOfJoinTablesRequest.MatchRule.GREATER_THAN);
            for (JoinMatch match : raj.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : raj.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            out = op(Builder::setAsOfJoin, builder.build());
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
                builder.addAllAggregates(AggregationAdapter.of(aggregation));
            }
            out = op(Builder::setComboAggregate, builder);
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

        public static List<Aggregate> of(Aggregation aggregation) {
            return aggregation.walk(new AggregationAdapter()).getOut();
        }

        private Aggregate out;
        private List<Aggregate> multi;

        public List<Aggregate> getOut() {
            if (multi != null) {
                return multi;
            }
            if (out != null) {
                return Collections.singletonList(out);
            }
            throw new IllegalStateException();
        }

        private Aggregate.Builder of(AggType type, Pair specs) {
            return Aggregate.newBuilder().setType(type).addMatchPairs(Strings.of(specs));
        }

        @Override
        public void visit(Min min) {
            out = of(AggType.MIN, min.pair()).build();
        }

        @Override
        public void visit(Max max) {
            out = of(AggType.MAX, max.pair()).build();
        }

        @Override
        public void visit(Sum sum) {
            out = of(AggType.SUM, sum.pair()).build();
        }

        @Override
        public void visit(Var var) {
            out = of(AggType.VAR, var.pair()).build();
        }

        @Override
        public void visit(Avg avg) {
            out = of(AggType.AVG, avg.pair()).build();
        }

        @Override
        public void visit(First first) {
            out = of(AggType.FIRST, first.pair()).build();
        }

        @Override
        public void visit(Last last) {
            out = of(AggType.LAST, last.pair()).build();
        }

        @Override
        public void visit(Std std) {
            out = of(AggType.STD, std.pair()).build();
        }

        @Override
        public void visit(Med med) {
            if (!med.averageMedian()) {
                throw new UnsupportedOperationException("TODO: need to plumb through");
            }
            out = of(AggType.MEDIAN, med.pair()).build();
        }

        @Override
        public void visit(Pct pct) {
            if (pct.averageMedian()) {
                throw new UnsupportedOperationException("TODO: need to plumb through");
            }
            out = of(AggType.PERCENTILE, pct.pair()).build();
        }

        @Override
        public void visit(WSum wSum) {
            throw new UnsupportedOperationException("TODO: need to plumb through");
        }

        @Override
        public void visit(WAvg wAvg) {
            out = of(AggType.WEIGHTED_AVG, wAvg.pair()).setColumnName(wAvg.weight().name()).build();
        }

        @Override
        public void visit(Count count) {
            out = Aggregate.newBuilder().setType(AggType.COUNT).setColumnName(count.column().name())
                .build();
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
            out = of(AggType.ARRAY, array.pair()).build();
        }

        @Override
        public void visit(AbsSum absSum) {
            out = of(AggType.ABS_SUM, absSum.pair()).build();
        }

        @Override
        public void visit(Unique unique) {
            throw new UnsupportedOperationException("TODO: need to plumb through");
        }

        @Override
        public void visit(SortedFirst sortedFirst) {
            throw new UnsupportedOperationException("TODO: need to plumb through");
        }

        @Override
        public void visit(SortedLast sortedLast) {
            throw new UnsupportedOperationException("TODO: need to plumb through");
        }

        @Override
        public void visit(Multi<?> multi) {
            List<Aggregate> result = new ArrayList<>();
            for (Aggregation aggregation : multi.aggregations()) {
                result.addAll(of(aggregation));
            }
            this.multi = result;
        }
    }
}
