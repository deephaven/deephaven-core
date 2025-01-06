//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import com.google.protobuf.ByteStringAccess;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.SortColumn.Order;
import io.deephaven.api.Strings;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterComparison.Operator;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.literal.Literal;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.proto.backplane.grpc.AggregateAllRequest;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.AjRajTablesRequest;
import io.deephaven.proto.backplane.grpc.AndCondition;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.Builder;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.CompareCondition.CompareOperation;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind.Blink;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind.InMemoryAppendOnly;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind.InMemoryKeyBacked;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.backplane.grpc.InCondition;
import io.deephaven.proto.backplane.grpc.IsNullCondition;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.proto.backplane.grpc.MultiJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.NotCondition;
import io.deephaven.proto.backplane.grpc.OrCondition;
import io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.Reference;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SliceRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortDescriptor.SortDirection;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.Value;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.qst.table.AggregateAllTable;
import io.deephaven.qst.table.AggregateTable;
import io.deephaven.qst.table.AsOfJoinTable;
import io.deephaven.qst.table.BlinkInputTable;
import io.deephaven.qst.table.Clock.Visitor;
import io.deephaven.qst.table.ClockSystem;
import io.deephaven.qst.table.DropColumnsTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.LazyUpdateTable;
import io.deephaven.qst.table.MergeTable;
import io.deephaven.qst.table.MultiJoinInput;
import io.deephaven.qst.table.MultiJoinTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.RangeJoinTable;
import io.deephaven.qst.table.ReverseTable;
import io.deephaven.qst.table.SelectDistinctTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.SliceTable;
import io.deephaven.qst.table.SnapshotTable;
import io.deephaven.qst.table.SnapshotWhenTable;
import io.deephaven.qst.table.SortTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSchema;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.UngroupTable;
import io.deephaven.qst.table.UpdateByTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereInTable;
import io.deephaven.qst.table.WhereTable;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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
            final Operation operation = table.walk(new OperationAdapter(ticket, indices, lookup));
            builder.addOps(operation);
            indices.put(table, ix++);
        }
        return builder.build();
    }

    private static <T> Operation op(BiFunction<Builder, T, Builder> f, T value) {
        return f.apply(Operation.newBuilder(), value).build();
    }

    private static class OperationAdapter implements TableSpec.Visitor<Operation> {
        private final Ticket ticket;
        private final Map<TableSpec, Integer> indices;
        private final ExportLookup lookup;

        OperationAdapter(Ticket ticket, Map<TableSpec, Integer> indices, ExportLookup lookup) {
            this.ticket = Objects.requireNonNull(ticket);
            this.indices = Objects.requireNonNull(indices);
            this.lookup = Objects.requireNonNull(lookup);
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
        public Operation visit(EmptyTable emptyTable) {
            return op(Builder::setEmptyTable,
                    EmptyTableRequest.newBuilder().setResultId(ticket).setSize(emptyTable.size()));
        }

        @Override
        public Operation visit(NewTable newTable) {
            throw new UnsupportedOperationException(
                    "TODO(deephaven-core#992): TableService implementation of NewTable, https://github.com/deephaven/deephaven-core/issues/992");
        }

        @Override
        public Operation visit(TimeTable timeTable) {
            // noinspection Convert2Lambda
            timeTable.clock().walk(new Visitor<Void>() {
                @Override
                public Void visit(ClockSystem system) {
                    // Even though this is functionally a no-op at the moment, it's good practice to
                    // include this visitor here since the number of TimeProvider implementations is
                    // expected to expand in the future.
                    return null;
                }
            });

            TimeTableRequest.Builder builder = TimeTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setPeriodNanos(timeTable.interval().toNanos())
                    .setBlinkTable(timeTable.blinkTable());
            if (timeTable.startTime().isPresent()) {
                final Instant startTime = timeTable.startTime().get();
                final long epochNanos = Math.addExact(
                        TimeUnit.SECONDS.toNanos(startTime.getEpochSecond()), startTime.getNano());
                builder.setStartTimeNanos(epochNanos);
            }
            return op(Builder::setTimeTable, builder.build());
        }

        @Override
        public Operation visit(MergeTable mergeTable) {
            MergeTablesRequest.Builder builder =
                    MergeTablesRequest.newBuilder().setResultId(ticket);
            for (TableSpec table : mergeTable.tables()) {
                builder.addSourceIds(ref(table));
            }
            return op(Builder::setMerge, builder.build());
        }

        @Override
        public Operation visit(HeadTable headTable) {
            return op(Builder::setHead, HeadOrTailRequest.newBuilder().setResultId(ticket)
                    .setSourceId(ref(headTable.parent())).setNumRows(headTable.size()));
        }

        @Override
        public Operation visit(TailTable tailTable) {
            return op(Builder::setTail, HeadOrTailRequest.newBuilder().setResultId(ticket)
                    .setSourceId(ref(tailTable.parent())).setNumRows(tailTable.size()));
        }

        @Override
        public Operation visit(SliceTable sliceTable) {
            return op(Builder::setSlice, SliceRequest.newBuilder().setResultId(ticket)
                    .setSourceId(ref(sliceTable.parent()))
                    .setFirstPositionInclusive(sliceTable.firstPositionInclusive())
                    .setLastPositionExclusive(sliceTable.lastPositionExclusive())
                    .build());
        }

        @Override
        public Operation visit(ReverseTable reverseTable) {
            // a bit hacky at the proto level, but this is how to specify a reverse
            return op(Builder::setSort,
                    SortTableRequest.newBuilder().setResultId(ticket)
                            .setSourceId(ref(reverseTable.parent()))
                            .addSorts(
                                    SortDescriptor.newBuilder().setDirection(SortDirection.REVERSE).build())
                            .build());
        }

        @Override
        public Operation visit(SortTable sortTable) {
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
            return op(Builder::setSort, builder.build());
        }

        @Override
        public Operation visit(SnapshotTable snapshotTable) {
            final SnapshotTableRequest.Builder builder = SnapshotTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(snapshotTable.parent()));
            return op(Builder::setSnapshot, builder.build());
        }

        @Override
        public Operation visit(SnapshotWhenTable snapshotWhenTable) {
            final SnapshotWhenOptions options = snapshotWhenTable.options();
            final SnapshotWhenTableRequest.Builder builder = SnapshotWhenTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setBaseId(ref(snapshotWhenTable.base()))
                    .setTriggerId(ref(snapshotWhenTable.trigger()))
                    .setInitial(options.has(Flag.INITIAL))
                    .setIncremental(options.has(Flag.INCREMENTAL))
                    .setHistory(options.has(Flag.HISTORY));
            for (JoinAddition stampColumn : options.stampColumns()) {
                builder.addStampColumns(Strings.of(stampColumn));
            }
            return op(Builder::setSnapshotWhen, builder.build());
        }

        private Operation createFilterTableRequest(WhereTable whereTable) {
            final FilterTableRequest.Builder builder = FilterTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(whereTable.parent()));
            for (Filter filter : Filter.extractAnds(whereTable.filter())) {
                builder.addFilters(FilterAdapter.of(filter));
            }
            return op(Builder::setFilter, builder.build());
        }

        private Operation createUnstructuredFilterTableRequest(WhereTable whereTable) {
            // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
            final UnstructuredFilterTableRequest.Builder builder = UnstructuredFilterTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(whereTable.parent()));
            for (Filter filter : Filter.extractAnds(whereTable.filter())) {
                builder.addFilters(Strings.of(filter));
            }
            return op(Builder::setUnstructuredFilter, builder.build());
        }

        @Override
        public Operation visit(WhereTable whereTable) {
            try {
                return createFilterTableRequest(whereTable);
            } catch (UnsupportedOperationException uoe) {
                // gRPC structures unable to support stronger typed versions.
                // Ignore exception, create unstructured version.
                return createUnstructuredFilterTableRequest(whereTable);
            }
        }

        @Override
        public Operation visit(WhereInTable whereInTable) {
            WhereInRequest.Builder builder = WhereInRequest.newBuilder()
                    .setResultId(ticket)
                    .setLeftId(ref(whereInTable.left()))
                    .setRightId(ref(whereInTable.right()))
                    .setInverted(whereInTable.inverted());
            for (JoinMatch match : whereInTable.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            return op(Builder::setWhereIn, builder.build());
        }

        @Override
        public Operation visit(NaturalJoinTable j) {
            NaturalJoinTablesRequest.Builder builder = NaturalJoinTablesRequest.newBuilder()
                    .setResultId(ticket).setLeftId(ref(j.left())).setRightId(ref(j.right()));
            for (JoinMatch match : j.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : j.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            return op(Builder::setNaturalJoin, builder.build());
        }

        @Override
        public Operation visit(ExactJoinTable j) {
            ExactJoinTablesRequest.Builder builder = ExactJoinTablesRequest.newBuilder()
                    .setResultId(ticket).setLeftId(ref(j.left())).setRightId(ref(j.right()));
            for (JoinMatch match : j.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : j.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            return op(Builder::setExactJoin, builder.build());
        }

        @Override
        public Operation visit(JoinTable j) {
            CrossJoinTablesRequest.Builder builder = CrossJoinTablesRequest.newBuilder()
                    .setResultId(ticket)
                    .setLeftId(ref(j.left()))
                    .setRightId(ref(j.right()));
            j.reserveBits().ifPresent(builder::setReserveBits);
            for (JoinMatch match : j.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : j.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            return op(Builder::setCrossJoin, builder.build());
        }

        @Override
        public Operation visit(AsOfJoinTable asOfJoin) {
            AjRajTablesRequest.Builder builder = AjRajTablesRequest.newBuilder()
                    .setResultId(ticket)
                    .setLeftId(ref(asOfJoin.left()))
                    .setRightId(ref(asOfJoin.right()));
            for (JoinMatch match : asOfJoin.matches()) {
                builder.addExactMatchColumns(Strings.of(match));
            }
            builder.setAsOfColumn(asOfJoin.joinMatch().toRpcString());
            for (JoinAddition addition : asOfJoin.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            return op(asOfJoin.isAj() ? Builder::setAj : Builder::setRaj, builder.build());
        }

        @Override
        public Operation visit(RangeJoinTable rangeJoinTable) {
            RangeJoinTablesRequest.Builder builder = RangeJoinTablesRequest.newBuilder()
                    .setResultId(ticket)
                    .setLeftId(ref(rangeJoinTable.left()))
                    .setRightId(ref(rangeJoinTable.right()));

            for (JoinMatch exactMatch : rangeJoinTable.exactMatches()) {
                builder.addExactMatchColumns(Strings.of(exactMatch));
            }

            builder.setLeftStartColumn(Strings.of(rangeJoinTable.rangeMatch().leftStartColumn()));
            final RangeJoinTablesRequest.RangeStartRule rangeStartRule;
            switch (rangeJoinTable.rangeMatch().rangeStartRule()) {
                case LESS_THAN:
                    rangeStartRule = RangeJoinTablesRequest.RangeStartRule.LESS_THAN;
                    break;
                case LESS_THAN_OR_EQUAL:
                    rangeStartRule = RangeJoinTablesRequest.RangeStartRule.LESS_THAN_OR_EQUAL;
                    break;
                case LESS_THAN_OR_EQUAL_ALLOW_PRECEDING:
                    rangeStartRule = RangeJoinTablesRequest.RangeStartRule.LESS_THAN_OR_EQUAL_ALLOW_PRECEDING;
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Unrecognized range start rule %s for range join",
                            rangeJoinTable.rangeMatch().rangeStartRule()));
            }
            builder.setRangeStartRule(rangeStartRule);
            builder.setRightRangeColumn(Strings.of(rangeJoinTable.rangeMatch().rightRangeColumn()));
            final RangeJoinTablesRequest.RangeEndRule rangeEndRule;
            switch (rangeJoinTable.rangeMatch().rangeEndRule()) {
                case GREATER_THAN:
                    rangeEndRule = RangeJoinTablesRequest.RangeEndRule.GREATER_THAN;
                    break;
                case GREATER_THAN_OR_EQUAL:
                    rangeEndRule = RangeJoinTablesRequest.RangeEndRule.GREATER_THAN_OR_EQUAL;
                    break;
                case GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING:
                    rangeEndRule = RangeJoinTablesRequest.RangeEndRule.GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING;
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Unrecognized range end rule %s for range join",
                            rangeJoinTable.rangeMatch().rangeEndRule()));
            }
            builder.setRangeEndRule(rangeEndRule);
            builder.setLeftEndColumn(Strings.of(rangeJoinTable.rangeMatch().leftEndColumn()));

            for (Aggregation aggregation : rangeJoinTable.aggregations()) {
                for (io.deephaven.proto.backplane.grpc.Aggregation adapted : AggregationBuilder.adapt(aggregation)) {
                    builder.addAggregations(adapted);
                }
            }

            return op(Builder::setRangeJoin, builder.build());
        }

        @Override
        public Operation visit(ViewTable v) {
            return op(Builder::setView, selectOrUpdate(v, v.columns()));
        }

        @Override
        public Operation visit(UpdateViewTable v) {
            return op(Builder::setUpdateView, selectOrUpdate(v, v.columns()));
        }

        @Override
        public Operation visit(UpdateTable v) {
            return op(Builder::setUpdate, selectOrUpdate(v, v.columns()));
        }

        @Override
        public Operation visit(LazyUpdateTable v) {
            return op(Builder::setLazyUpdate, selectOrUpdate(v, v.columns()));
        }

        @Override
        public Operation visit(SelectTable v) {
            return op(Builder::setSelect, selectOrUpdate(v, v.columns()));
        }

        @Override
        public Operation visit(AggregateAllTable aggregateAllTable) {
            AggregateAllRequest.Builder builder = AggregateAllRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(aggregateAllTable.parent()))
                    .setSpec(AggSpecBuilder.adapt(aggregateAllTable.spec()));
            for (ColumnName column : aggregateAllTable.groupByColumns()) {
                builder.addGroupByColumns(Strings.of(column));
            }
            return op(Builder::setAggregateAll, builder.build());
        }

        @Override
        public Operation visit(AggregateTable aggregateTable) {
            AggregateRequest.Builder builder = AggregateRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(aggregateTable.parent()))
                    .setPreserveEmpty(aggregateTable.preserveEmpty());
            aggregateTable
                    .initialGroups()
                    .map(this::ref)
                    .ifPresent(builder::setInitialGroupsId);
            for (Aggregation aggregation : aggregateTable.aggregations()) {
                for (io.deephaven.proto.backplane.grpc.Aggregation adapted : AggregationBuilder.adapt(aggregation)) {
                    builder.addAggregations(adapted);
                }
            }
            for (ColumnName column : aggregateTable.groupByColumns()) {
                builder.addGroupByColumns(Strings.of(column));
            }
            return op(Builder::setAggregate, builder.build());
        }

        @Override
        public Operation visit(TicketTable ticketTable) {
            Ticket sourceTicket = Ticket.newBuilder().setTicket(ByteStringAccess.wrap(ticketTable.ticket())).build();
            TableReference sourceReference = TableReference.newBuilder().setTicket(sourceTicket).build();
            FetchTableRequest.Builder builder =
                    FetchTableRequest.newBuilder().setResultId(ticket).setSourceId(sourceReference);
            return op(Builder::setFetchTable, builder);
        }

        @Override
        public Operation visit(InputTable inputTable) {
            CreateInputTableRequest.Builder builder = CreateInputTableRequest.newBuilder()
                    .setResultId(ticket);
            inputTable.schema().walk(new TableSchema.Visitor<Void>() {
                @Override
                public Void visit(TableSpec spec) {
                    builder.setSourceTableId(ref(spec));
                    return null;
                }

                @Override
                public Void visit(TableHeader header) {
                    builder.setSchema(ByteStringAccess.wrap(SchemaBytes.of(header)));
                    return null;
                }
            });
            builder.setKind(inputTable.walk(new InputTable.Visitor<InputTableKind>() {
                @Override
                public InputTableKind visit(InMemoryAppendOnlyInputTable inMemoryAppendOnly) {
                    return InputTableKind.newBuilder().setInMemoryAppendOnly(InMemoryAppendOnly.newBuilder()).build();
                }

                @Override
                public InputTableKind visit(InMemoryKeyBackedInputTable inMemoryKeyBacked) {
                    return InputTableKind.newBuilder().setInMemoryKeyBacked(
                            InMemoryKeyBacked.newBuilder().addAllKeyColumns(inMemoryKeyBacked.keys())).build();
                }

                @Override
                public InputTableKind visit(BlinkInputTable blinkInputTable) {
                    return InputTableKind.newBuilder().setBlink(Blink.getDefaultInstance()).build();
                }
            }));
            return op(Builder::setCreateInputTable, builder);
        }

        @Override
        public Operation visit(SelectDistinctTable selectDistinctTable) {
            return op(Builder::setSelectDistinct, selectDistinct(selectDistinctTable));
        }

        @Override
        public Operation visit(UpdateByTable updateByTable) {
            final UpdateByRequest.Builder request = UpdateByBuilder
                    .adapt(updateByTable)
                    .setResultId(ticket)
                    .setSourceId(ref(updateByTable.parent()));
            return op(Builder::setUpdateBy, request);
        }

        @Override
        public Operation visit(UngroupTable ungroupTable) {
            final UngroupRequest.Builder request = UngroupRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(ungroupTable.parent()))
                    .setNullFill(ungroupTable.nullFill());
            for (ColumnName ungroupColumn : ungroupTable.ungroupColumns()) {
                request.addColumnsToUngroup(ungroupColumn.name());
            }
            return op(Builder::setUngroup, request);
        }

        @Override
        public Operation visit(DropColumnsTable dropColumnsTable) {
            final DropColumnsRequest.Builder request = DropColumnsRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(dropColumnsTable.parent()));
            for (ColumnName dropColumn : dropColumnsTable.dropColumns()) {
                request.addColumnNames(dropColumn.name());
            }
            return op(Builder::setDropColumns, request);
        }

        @Override
        public Operation visit(MultiJoinTable multiJoinTable) {
            final MultiJoinTablesRequest.Builder request = MultiJoinTablesRequest.newBuilder()
                    .setResultId(ticket);
            for (MultiJoinInput<TableSpec> input : multiJoinTable.inputs()) {
                request.addMultiJoinInputs(adapt(input));
            }
            return op(Builder::setMultiJoin, request);
        }

        private io.deephaven.proto.backplane.grpc.MultiJoinInput adapt(MultiJoinInput<TableSpec> input) {
            io.deephaven.proto.backplane.grpc.MultiJoinInput.Builder builder =
                    io.deephaven.proto.backplane.grpc.MultiJoinInput.newBuilder()
                            .setSourceId(ref(input.table()));
            for (JoinMatch match : input.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            for (JoinAddition addition : input.additions()) {
                builder.addColumnsToAdd(Strings.of(addition));
            }
            return builder.build();
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

        private SelectDistinctRequest selectDistinct(SelectDistinctTable selectDistinctTable) {
            SelectDistinctRequest.Builder builder = SelectDistinctRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(selectDistinctTable.parent()));
            for (Selectable column : selectDistinctTable.columns()) {
                builder.addColumnNames(Strings.of(column));
            }
            return builder.build();
        }
    }

    private static Reference reference(ColumnName columnName) {
        return Reference.newBuilder().setColumnName(columnName.name()).build();
    }

    private static io.deephaven.proto.backplane.grpc.Literal literal(long x) {
        return io.deephaven.proto.backplane.grpc.Literal.newBuilder().setLongValue(x).build();
    }

    private static io.deephaven.proto.backplane.grpc.Literal literal(boolean x) {
        return io.deephaven.proto.backplane.grpc.Literal.newBuilder().setBoolValue(x).build();
    }

    private static io.deephaven.proto.backplane.grpc.Literal literal(double x) {
        return io.deephaven.proto.backplane.grpc.Literal.newBuilder().setDoubleValue(x).build();
    }

    private static io.deephaven.proto.backplane.grpc.Literal literal(String x) {
        return io.deephaven.proto.backplane.grpc.Literal.newBuilder().setStringValue(x).build();
    }

    static class ExpressionAdapter implements Expression.Visitor<Value>, Literal.Visitor<Value> {
        static Value adapt(Expression expression) {
            return expression.walk(new ExpressionAdapter());
        }

        static Value adapt(Literal literal) {
            return literal.walk((Literal.Visitor<Value>) new ExpressionAdapter());
        }

        @Override
        public Value visit(ColumnName x) {
            return Value.newBuilder().setReference(reference(x)).build();
        }

        @Override
        public Value visit(char literal) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Value does not support literal char");
        }

        @Override
        public Value visit(byte literal) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Value does not support literal byte");
        }

        @Override
        public Value visit(short literal) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Value does not support literal short");
        }

        @Override
        public Value visit(int literal) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Value does not support literal int");
        }

        @Override
        public Value visit(long literal) {
            return Value.newBuilder().setLiteral(literal(literal)).build();
        }

        @Override
        public Value visit(boolean literal) {
            return Value.newBuilder().setLiteral(literal(literal)).build();
        }

        @Override
        public Value visit(float literal) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Value does not support literal float");
        }

        @Override
        public Value visit(double literal) {
            return Value.newBuilder().setLiteral(literal(literal)).build();
        }

        @Override
        public Value visit(String literal) {
            return Value.newBuilder().setLiteral(literal(literal)).build();
        }

        @Override
        public Value visit(Literal literal) {
            return literal.walk((Literal.Visitor<Value>) this);
        }

        @Override
        public Value visit(Filter filter) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException(
                    "Unable to create a io.deephaven.proto.backplane.grpc.Value from a Filter");
        }

        @Override
        public Value visit(Function function) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException(
                    "Unable to create a io.deephaven.proto.backplane.grpc.Value from a Function");
        }

        @Override
        public Value visit(Method method) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException(
                    "Unable to create a io.deephaven.proto.backplane.grpc.Value from a Method");
        }

        @Override
        public Value visit(RawString rawString) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException(
                    "Unable to create a io.deephaven.proto.backplane.grpc.Value from a raw string");
        }
    }

    enum LiteralAdapter implements Literal.Visitor<io.deephaven.proto.backplane.grpc.Literal> {
        INSTANCE;

        public static io.deephaven.proto.backplane.grpc.Literal of(Literal literal) {
            return literal.walk(INSTANCE);
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(boolean literal) {
            return literal(literal);
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(char literal) {
            throw new UnsupportedOperationException("Doesn't support char literal");
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(byte literal) {
            throw new UnsupportedOperationException("Doesn't support byte literal");
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(short literal) {
            throw new UnsupportedOperationException("Doesn't support short literal");
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(int literal) {
            throw new UnsupportedOperationException("Doesn't support int literal");
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(long literal) {
            return literal(literal);
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(float literal) {
            throw new UnsupportedOperationException("Doesn't support float literal");
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(double literal) {
            return literal(literal);
        }

        @Override
        public io.deephaven.proto.backplane.grpc.Literal visit(String literal) {
            return literal(literal);
        }
    }

    static class FilterAdapter implements Filter.Visitor<Condition> {

        static Condition of(Filter filter) {
            return filter.walk(new FilterAdapter());
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

        @Override
        public Condition visit(FilterIsNull isNull) {
            if (!(isNull.expression() instanceof ColumnName)) {
                // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
                throw new UnsupportedOperationException("Only supports null checking a reference to a column");
            }
            return Condition.newBuilder()
                    .setIsNull(IsNullCondition.newBuilder().setReference(reference((ColumnName) isNull.expression()))
                            .build())
                    .build();
        }

        @Override
        public Condition visit(FilterComparison comparison) {
            FilterComparison preferred = comparison.maybeTranspose();
            Operator operator = preferred.operator();
            // Processing as single FilterIn is currently the more efficient server impl.
            // See FilterTableGrpcImpl
            // See io.deephaven.server.table.ops.filter.FilterFactory
            switch (operator) {
                case EQUALS:
                    return visit(FilterIn.of(preferred.lhs(), preferred.rhs()));
                case NOT_EQUALS:
                    return visit(Filter.not(FilterIn.of(preferred.lhs(), preferred.rhs())));
            }
            return Condition.newBuilder()
                    .setCompare(CompareCondition.newBuilder()
                            .setOperation(adapt(operator))
                            .setLhs(ExpressionAdapter.adapt(preferred.lhs()))
                            .setRhs(ExpressionAdapter.adapt(preferred.rhs()))
                            .build())
                    .build();
        }

        @Override
        public Condition visit(FilterIn in) {
            final InCondition.Builder builder = InCondition.newBuilder()
                    .setTarget(ExpressionAdapter.adapt(in.expression()));
            for (Expression value : in.values()) {
                builder.addCandidates(ExpressionAdapter.adapt(value));
            }
            return Condition.newBuilder().setIn(builder).build();
        }

        @Override
        public Condition visit(FilterNot<?> not) {
            // This is a shallow simplification that removes the need for setNot when it is not needed.
            final Filter invertedFilter = not.invertFilter();
            if (not.equals(invertedFilter)) {
                return Condition.newBuilder().setNot(NotCondition.newBuilder().setFilter(of(not.filter())).build())
                        .build();
            } else {
                return of(invertedFilter);
            }
        }

        @Override
        public Condition visit(FilterOr ors) {
            OrCondition.Builder builder = OrCondition.newBuilder();
            for (Filter filter : ors) {
                builder.addFilters(of(filter));
            }
            return Condition.newBuilder().setOr(builder.build()).build();
        }

        @Override
        public Condition visit(FilterAnd ands) {
            AndCondition.Builder builder = AndCondition.newBuilder();
            for (Filter filter : ands) {
                builder.addFilters(of(filter));
            }
            return Condition.newBuilder().setAnd(builder.build()).build();
        }

        @Override
        public Condition visit(FilterPattern pattern) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Can't build Condition with FilterPattern");
        }

        @Override
        public Condition visit(Function function) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Can't build Condition with Function");
        }

        @Override
        public Condition visit(Method method) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Can't build Condition with Method");
        }

        @Override
        public Condition visit(boolean literal) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Can't build Condition with literal");
        }

        @Override
        public Condition visit(RawString rawString) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Can't build Condition with raw string");
        }
    }
}
