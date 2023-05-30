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
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.api.literal.Literal;
import io.deephaven.proto.backplane.grpc.AggregateAllRequest;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.AndCondition;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.Builder;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.CompareCondition.CompareOperation;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest;
import io.deephaven.proto.backplane.grpc.CreateInputTableRequest.InputTableKind;
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
import io.deephaven.proto.backplane.grpc.MetaTableRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.NotCondition;
import io.deephaven.proto.backplane.grpc.OrCondition;
import io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.Reference;
import io.deephaven.proto.backplane.grpc.SearchCondition;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
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
import io.deephaven.qst.table.MetaTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.RangeJoinTable;
import io.deephaven.qst.table.ReverseAsOfJoinTable;
import io.deephaven.qst.table.ReverseTable;
import io.deephaven.qst.table.SelectDistinctTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SingleParentTable;
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
            timeTable.clock().walk(new Visitor() {
                @Override
                public void visit(ClockSystem system) {
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
            final SnapshotTableRequest.Builder builder = SnapshotTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(snapshotTable.parent()));
            out = op(Builder::setSnapshot, builder.build());
        }

        @Override
        public void visit(SnapshotWhenTable snapshotWhenTable) {
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
            out = op(Builder::setSnapshotWhen, builder.build());
        }

        private Operation createFilterTableRequest(WhereTable whereTable) {
            FilterTableRequest request = FilterTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(whereTable.parent()))
                    .addFilters(FilterAdapter.of(whereTable.filter()))
                    .build();
            return op(Builder::setFilter, request);
        }

        private Operation createUnstructuredFilterTableRequest(WhereTable whereTable) {
            // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
            UnstructuredFilterTableRequest request = UnstructuredFilterTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(whereTable.parent()))
                    .addFilters(Strings.of(whereTable.filter()))
                    .build();
            return op(Builder::setUnstructuredFilter, request);
        }

        @Override
        public void visit(WhereTable whereTable) {
            try {
                out = createFilterTableRequest(whereTable);
            } catch (UnsupportedOperationException uoe) {
                // gRPC structures unable to support stronger typed versions.
                // Ignore exception, create unstructured version.
                out = createUnstructuredFilterTableRequest(whereTable);
            }
        }

        @Override
        public void visit(WhereInTable whereInTable) {
            WhereInRequest.Builder builder = WhereInRequest.newBuilder()
                    .setResultId(ticket)
                    .setLeftId(ref(whereInTable.left()))
                    .setRightId(ref(whereInTable.right()))
                    .setInverted(whereInTable.inverted());
            for (JoinMatch match : whereInTable.matches()) {
                builder.addColumnsToMatch(Strings.of(match));
            }
            out = op(Builder::setWhereIn, builder.build());
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
            out = op(Builder::setCrossJoin, builder.build());
        }

        @Override
        public void visit(AsOfJoinTable aj) {
            AsOfJoinTablesRequest.Builder builder = AsOfJoinTablesRequest.newBuilder()
                    .setResultId(ticket)
                    .setLeftId(ref(aj.left()))
                    .setRightId(ref(aj.right()))
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
                    .setResultId(ticket)
                    .setLeftId(ref(raj.left()))
                    .setRightId(ref(raj.right()))
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
        public void visit(RangeJoinTable rangeJoinTable) {
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

            out = op(Builder::setRangeJoin, builder.build());
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
        public void visit(AggregateAllTable aggregateAllTable) {
            AggregateAllRequest.Builder builder = AggregateAllRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(aggregateAllTable.parent()))
                    .setSpec(AggSpecBuilder.adapt(aggregateAllTable.spec()));
            for (ColumnName column : aggregateAllTable.groupByColumns()) {
                builder.addGroupByColumns(Strings.of(column));
            }
            out = op(Builder::setAggregateAll, builder.build());
        }

        @Override
        public void visit(AggregateTable aggregateTable) {
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
            out = op(Builder::setAggregate, builder.build());
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
        public void visit(UpdateByTable updateByTable) {
            final UpdateByRequest.Builder request = UpdateByBuilder
                    .adapt(updateByTable)
                    .setResultId(ticket)
                    .setSourceId(ref(updateByTable.parent()));
            out = op(Builder::setUpdateBy, request);
        }

        @Override
        public void visit(UngroupTable ungroupTable) {
            final UngroupRequest.Builder request = UngroupRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(ungroupTable.parent()))
                    .setNullFill(ungroupTable.nullFill());
            for (ColumnName ungroupColumn : ungroupTable.ungroupColumns()) {
                request.addColumnsToUngroup(ungroupColumn.name());
            }
            out = op(Builder::setUngroup, request);
        }

        @Override
        public void visit(DropColumnsTable dropColumnsTable) {
            final DropColumnsRequest.Builder request = DropColumnsRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(dropColumnsTable.parent()));
            for (ColumnName dropColumn : dropColumnsTable.dropColumns()) {
                request.addColumnNames(dropColumn.name());
            }
            out = op(Builder::setDropColumns, request);
        }

        @Override
        public void visit(MetaTable metaTable) {
            final MetaTableRequest request = MetaTableRequest.newBuilder()
                    .setResultId(ticket)
                    .setSourceId(ref(metaTable.parent()))
                    .build();
            out = op(Builder::setMetaTable, request);
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
            return Condition.newBuilder()
                    .setCompare(CompareCondition.newBuilder()
                            .setOperation(adapt(preferred.operator()))
                            .setLhs(ExpressionAdapter.adapt(preferred.lhs()))
                            .setRhs(ExpressionAdapter.adapt(preferred.rhs()))
                            .build())
                    .build();
        }

        @Override
        public Condition visit(FilterIn in) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Can't build Condition with FilterIn");
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
