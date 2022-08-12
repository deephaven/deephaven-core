/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import com.google.protobuf.ByteStringAccess;
import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.RawString;
import io.deephaven.api.ReverseAsOfJoinRule;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.SortColumn.Order;
import io.deephaven.api.Strings;
import io.deephaven.api.agg.*;
import io.deephaven.api.agg.spec.*;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterCondition;
import io.deephaven.api.filter.FilterCondition.Operator;
import io.deephaven.api.filter.FilterIsNotNull;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.value.Value;
import io.deephaven.proto.backplane.grpc.AndCondition;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.Builder;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest.AggType;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest.Aggregate;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.CompareCondition.CompareOperation;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind.InMemoryAppendOnly;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind.InMemoryKeyBacked;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.backplane.grpc.IsNullCondition;
import io.deephaven.proto.backplane.grpc.Literal;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.NotCondition;
import io.deephaven.proto.backplane.grpc.OrCondition;
import io.deephaven.proto.backplane.grpc.Reference;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortDescriptor.SortDirection;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.qst.table.AggregateAllByTable;
import io.deephaven.qst.table.AggregationTable;
import io.deephaven.qst.table.AsOfJoinTable;
import io.deephaven.qst.table.ByTableBase;
import io.deephaven.qst.table.CountByTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.LazyUpdateTable;
import io.deephaven.qst.table.MergeTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.ReverseAsOfJoinTable;
import io.deephaven.qst.table.ReverseTable;
import io.deephaven.qst.table.SelectDistinctTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.SnapshotTable;
import io.deephaven.qst.table.SortTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSchema;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeProvider.Visitor;
import io.deephaven.qst.table.TimeProviderSystem;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.UpdateByTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereInTable;
import io.deephaven.qst.table.WhereNotInTable;
import io.deephaven.qst.table.WhereTable;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

class BatchTableRequestBuilder {

    interface ExportLookup {
        OptionalInt ticket(TableSpec spec);
    }

    static BatchTableRequest buildNoChecks(ExportLookup lookup, Collection<TableSpec> postOrder) {
        final Map<TableSpec, Integer> indices = new HashMap<>(postOrder.size());
        final BatchTableRequest.Builder builder = BatchTableRequest.newBuilder();
        int ix = 0;
        for (TableSpec table : postOrder) {
            final OptionalInt exportId = lookup.ticket(table);
            final Ticket ticket =
                    exportId.isPresent() ? ExportTicketHelper.wrapExportIdInTicket(exportId.getAsInt())
                            : Ticket.getDefaultInstance();
            final Operation operation =
                    table.walk(new OperationAdapter(ticket, indices, lookup)).getOut();
            builder.addOps(operation);
            indices.put(table, ix++);
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
            OptionalInt existing = lookup.ticket(table);
            if (existing.isPresent()) {
                return ExportTicketHelper.tableReference(existing.getAsInt());
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
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#992): TableService implementation of NewTable, https://github.com/deephaven/deephaven-core/issues/992");
        }

        @Override
        public void visit(TimeTable timeTable) {
            // noinspection Convert2Lambda
            timeTable.timeProvider().walk(new Visitor() {
                @Override
                public void visit(TimeProviderSystem system) {
                    // Even though this is functionally a no-op at the moment, it's good practice to
                    // include this visitor here since the number of TimeProvider implementations is
                    // expected to expand in the future.
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
            if (whereTable.hasRawFilter()) {
                UnstructuredFilterTableRequest.Builder builder = UnstructuredFilterTableRequest
                        .newBuilder().setResultId(ticket).setSourceId(ref(whereTable.parent()));
                for (Filter filter : whereTable.filters()) {
                    builder.addFilters(Strings.of(filter));
                }
                out = op(Builder::setUnstructuredFilter, builder.build());
            } else {
                FilterTableRequest.Builder builder = FilterTableRequest.newBuilder()
                        .setResultId(ticket).setSourceId(ref(whereTable.parent()));
                for (Filter filter : whereTable.filters()) {
                    builder.addFilters(FilterAdapter.of(filter));
                }
                out = op(Builder::setFilter, builder.build());
            }
        }

        @Override
        public void visit(WhereInTable whereInTable) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#990): TableService implementation of whereIn/whereNotIn, https://github.com/deephaven/deephaven-core/issues/990");
        }

        @Override
        public void visit(WhereNotInTable whereNotInTable) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#990): TableService implementation of whereIn/whereNotIn, https://github.com/deephaven/deephaven-core/issues/990");
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
        public void visit(LazyUpdateTable v) {
            out = op(Builder::setLazyUpdate, selectOrUpdate(v, v.columns()));
        }

        @Override
        public void visit(SelectTable v) {
            out = op(Builder::setSelect, selectOrUpdate(v, v.columns()));
        }

        @Override
        public void visit(AggregateAllByTable aggAllByTable) {
            out = op(Builder::setComboAggregate, aggAllBy(aggAllByTable));
        }

        @Override
        public void visit(AggregationTable aggregationTable) {
            if (aggregationTable.preserveEmpty() || aggregationTable.initialGroups().isPresent()) {
                throw new UnsupportedOperationException(
                        "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
            }
            out = op(Builder::setComboAggregate, aggBy(aggregationTable));
        }

        @Override
        public void visit(TicketTable ticketTable) {
            Ticket sourceTicket = Ticket.newBuilder().setTicket(ByteStringAccess.wrap(ticketTable.ticket())).build();
            TableReference sourceReference = TableReference.newBuilder().setTicket(sourceTicket).build();
            FetchTableRequest.Builder builder =
                    FetchTableRequest.newBuilder().setResultId(ticket).setSourceId(sourceReference);
            out = op(Builder::setFetchTable, builder);
        }

        @Override
        public void visit(InputTable inputTable) {
            CreateInputTableRequest.Builder builder = CreateInputTableRequest.newBuilder()
                    .setResultId(ticket);
            inputTable.schema().walk(new TableSchema.Visitor() {
                @Override
                public void visit(TableSpec spec) {
                    builder.setSourceTableId(ref(spec));
                }

                @Override
                public void visit(TableHeader header) {
                    builder.setSchema(ByteStringAccess.wrap(SchemaBytes.of(header)));
                }
            });
            inputTable.walk(new InputTable.Visitor() {
                @Override
                public void visit(InMemoryAppendOnlyInputTable inMemoryAppendOnly) {
                    builder.setKind(InputTableKind.newBuilder().setInMemoryAppendOnly(InMemoryAppendOnly.newBuilder()));
                }

                @Override
                public void visit(InMemoryKeyBackedInputTable inMemoryKeyBacked) {
                    builder.setKind(InputTableKind.newBuilder().setInMemoryKeyBacked(
                            InMemoryKeyBacked.newBuilder().addAllKeyColumns(inMemoryKeyBacked.keys())));
                }
            });
            out = op(Builder::setCreateInputTable, builder);
        }

        @Override
        public void visit(SelectDistinctTable selectDistinctTable) {
            out = op(Builder::setSelectDistinct, selectDistinct(selectDistinctTable));
        }

        @Override
        public void visit(CountByTable countByTable) {
            out = op(Builder::setComboAggregate, countBy(countByTable));
        }

        @Override
        public void visit(UpdateByTable updateByTable) {
            throw new UnsupportedOperationException("TODO(deephaven-core#2607): UpdateByTable gRPC impl");
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

        private ComboAggregateRequest aggAllBy(AggregateAllByTable agg) {
            // A single aggregate without any input/output is how the protocol knows this is an "aggregate all by"
            // as opposed to an AggregationTable with one agg.
            final Aggregate aggregate = agg.spec().walk(new AggregateAdapter(Collections.emptyList())).out();
            return groupByColumns(agg).addAggregates(aggregate).build();
        }

        private ComboAggregateRequest aggBy(AggregationTable agg) {
            ComboAggregateRequest.Builder builder = groupByColumns(agg);
            for (Aggregation aggregation : agg.aggregations()) {
                AggregationAdapter.of(aggregation).forEach(builder::addAggregates);
            }
            return builder.build();
        }

        private ComboAggregateRequest.Builder groupByColumns(ByTableBase base) {
            ComboAggregateRequest.Builder builder = ComboAggregateRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(base.parent()));
            for (Selectable column : base.groupByColumns()) {
                builder.addGroupByColumns(Strings.of(column));
            }
            return builder;
        }

        private SelectDistinctRequest selectDistinct(SelectDistinctTable selectDistinctTable) {
            SelectDistinctRequest.Builder builder = SelectDistinctRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(selectDistinctTable.parent()));
            for (Selectable column : selectDistinctTable.columns()) {
                builder.addColumnNames(Strings.of(column));
            }
            return builder.build();
        }

        private ComboAggregateRequest countBy(CountByTable countByTable) {
            final Aggregate aggregate = Aggregate.newBuilder()
                    .setType(AggType.COUNT)
                    .setColumnName(countByTable.countName().name())
                    .build();
            return groupByColumns(countByTable).addAggregates(aggregate).build();
        }
    }

    private static class AggregationAdapter implements Aggregation.Visitor {

        public static Stream<Aggregate> of(Aggregation aggregation) {
            return aggregation.walk(new AggregationAdapter()).out();
        }

        private Stream<Aggregate> out;

        public Stream<Aggregate> out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(Aggregations aggregations) {
            out = aggregations.aggregations().stream().flatMap(AggregationAdapter::of);
        }

        @Override
        public void visit(Count count) {
            out = Stream.of(Aggregate.newBuilder().setType(AggType.COUNT).setColumnName(count.column().name()).build());
        }

        @Override
        public void visit(FirstRowKey firstRowKey) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(LastRowKey lastRowKey) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(Partition partition) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(ColumnAggregation columnAgg) {
            out = Stream.of(columnAgg.spec()
                    .walk(new AggregateAdapter(Collections.singletonList(columnAgg.pair()))).out());
        }

        @Override
        public void visit(ColumnAggregations columnAggs) {
            out = Stream.of(columnAggs.spec().walk(new AggregateAdapter(columnAggs.pairs())).out());
        }
    }

    private static Reference reference(ColumnName columnName) {
        return Reference.newBuilder().setColumnName(columnName.name()).build();
    }

    private static Literal literal(long x) {
        return Literal.newBuilder().setLongValue(x).build();
    }

    static class ValueAdapter implements Value.Visitor {
        static io.deephaven.proto.backplane.grpc.Value adapt(Value value) {
            return value.walk(new ValueAdapter()).out();
        }

        private io.deephaven.proto.backplane.grpc.Value out;

        public io.deephaven.proto.backplane.grpc.Value out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName x) {
            out = io.deephaven.proto.backplane.grpc.Value.newBuilder().setReference(reference(x))
                    .build();
        }

        @Override
        public void visit(long x) {
            out =
                    io.deephaven.proto.backplane.grpc.Value.newBuilder().setLiteral(literal(x)).build();
        }
    }

    static class FilterAdapter implements Filter.Visitor {

        static Condition of(Filter filter) {
            return filter.walk(new FilterAdapter()).out();
        }

        private static CompareOperation adapt(Operator operator) {
            switch (operator) {
                case LESS_THAN:
                    return CompareOperation.LESS_THAN;
                case LESS_THAN_OR_EQUAL:
                    return CompareOperation.LESS_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return CompareOperation.GREATER_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return CompareOperation.GREATER_THAN_OR_EQUAL;
                case EQUALS:
                    return CompareOperation.EQUALS;
                case NOT_EQUALS:
                    return CompareOperation.NOT_EQUALS;
                default:
                    throw new IllegalArgumentException("Unexpected operator " + operator);
            }
        }

        private Condition out;

        public Condition out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(FilterIsNull isNull) {
            out = Condition.newBuilder()
                    .setIsNull(
                            IsNullCondition.newBuilder().setReference(reference(isNull.column())).build())
                    .build();
        }

        @Override
        public void visit(FilterIsNotNull isNotNull) {
            out = of(FilterIsNull.of(isNotNull.column()).not());
        }

        @Override
        public void visit(FilterCondition condition) {
            FilterCondition preferred = condition.maybeTranspose();
            out = Condition.newBuilder()
                    .setCompare(CompareCondition.newBuilder().setOperation(adapt(preferred.operator()))
                            .setLhs(ValueAdapter.adapt(preferred.lhs()))
                            .setRhs(ValueAdapter.adapt(preferred.rhs())).build())
                    .build();
        }

        @Override
        public void visit(FilterNot not) {
            out = Condition.newBuilder()
                    .setNot(NotCondition.newBuilder().setFilter(of(not.filter())).build()).build();
        }

        @Override
        public void visit(FilterOr ors) {
            OrCondition.Builder builder = OrCondition.newBuilder();
            for (Filter filter : ors) {
                builder.addFilters(of(filter));
            }
            out = Condition.newBuilder().setOr(builder.build()).build();
        }

        @Override
        public void visit(FilterAnd ands) {
            AndCondition.Builder builder = AndCondition.newBuilder();
            for (Filter filter : ands) {
                builder.addFilters(of(filter));
            }
            out = Condition.newBuilder().setAnd(builder.build()).build();
        }

        @Override
        public void visit(RawString rawString) {
            throw new IllegalStateException("Can't build Condition with raw string");
        }
    }

    private static Aggregate.Builder of(AggType type, List<Pair> pairs) {
        final Aggregate.Builder builder = Aggregate.newBuilder().setType(type);
        for (Pair pair : pairs) {
            builder.addMatchPairs(Strings.of(pair));
        }
        return builder;
    }

    private static class AggregateAdapter implements AggSpec.Visitor {

        private final List<Pair> pairs;

        public AggregateAdapter(List<Pair> pairs) {
            this.pairs = Objects.requireNonNull(pairs);
        }

        private Aggregate out;

        private Aggregate out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(AggSpecAbsSum absSum) {
            out = of(AggType.ABS_SUM, pairs).build();
        }

        @Override
        public void visit(AggSpecApproximatePercentile approxPct) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecAvg avg) {
            out = of(AggType.AVG, pairs).build();
        }

        @Override
        public void visit(AggSpecCountDistinct countDistinct) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecDistinct distinct) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecFirst first) {
            out = of(AggType.FIRST, pairs).build();
        }

        @Override
        public void visit(AggSpecFormula formula) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecFreeze freeze) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecGroup group) {
            out = of(AggType.GROUP, pairs).build();
        }

        @Override
        public void visit(AggSpecLast last) {
            out = of(AggType.LAST, pairs).build();
        }

        @Override
        public void visit(AggSpecMax max) {
            out = of(AggType.MAX, pairs).build();
        }

        @Override
        public void visit(AggSpecMedian median) {
            if (!median.averageEvenlyDivided()) {
                throw new UnsupportedOperationException(
                        "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
            }
            out = of(AggType.MEDIAN, pairs).build();
        }

        @Override
        public void visit(AggSpecMin min) {
            out = of(AggType.MIN, pairs).build();
        }

        @Override
        public void visit(AggSpecPercentile pct) {
            if (pct.averageEvenlyDivided()) {
                throw new UnsupportedOperationException(
                        "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
            }
            out = of(AggType.PERCENTILE, pairs).build();
        }

        @Override
        public void visit(AggSpecSortedFirst sortedFirst) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecSortedLast sortedLast) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecStd std) {
            out = of(AggType.STD, pairs).build();
        }

        @Override
        public void visit(AggSpecSum sum) {
            out = of(AggType.SUM, pairs).build();
        }

        @Override
        public void visit(AggSpecTDigest tDigest) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecUnique unique) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecWAvg wAvg) {
            out = of(AggType.WEIGHTED_AVG, pairs).setColumnName(wAvg.weight().name()).build();

        }

        @Override
        public void visit(AggSpecWSum wSum) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#991): TableService aggregation coverage, https://github.com/deephaven/deephaven-core/issues/991");
        }

        @Override
        public void visit(AggSpecVar var) {
            out = of(AggType.VAR, pairs).build();
        }
    }
}
