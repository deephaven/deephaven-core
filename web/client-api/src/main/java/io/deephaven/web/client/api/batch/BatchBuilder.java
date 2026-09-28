//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.batch;

import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.proto.backplane.grpc.FlattenRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.web.client.api.Sort;
import io.deephaven.web.client.api.TableTicket;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.CustomColumnDescriptor;
import io.deephaven.web.shared.fu.MappedIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Used by client to create batched requests.
 *
 * This allows us to store everything using the objects a client expects to interact with (Sort, FilterCondition, etc),
 * rather than DTO types like SortDescriptor, FilterDescriptor, etc.
 *
 */
public class BatchBuilder {

    private List<BatchOp> ops = new ArrayList<>();
    private BatchOp next;

    public static class BatchOp extends TableConfig {

        public BatchOp() {}

        private TableTicket source, target;
        private ClientTableState state;
        private ClientTableState appendTo;

        public boolean hasHandles() {
            return source != null && target != null;
        }

        public void setHandles(TableTicket source, TableTicket target) {
            this.source = source;
            this.target = target;
        }

        public TableTicket getNewId() {
            return target == null ? state.getHandle() : target;
        }

        public TableTicket getSource() {
            return source == null ? appendTo == null ? null : appendTo.getHandle() : source;
        }

        public ClientTableState getState() {
            return state;
        }

        public void setState(ClientTableState state) {
            this.state = state;
        }

        public void fromState(ClientTableState state) {
            setState(state);
            setSorts(state.getSorts());
            setConditions(state.getConditions());
            setDropColumns(state.getDropColumns());
            setViewColumns(state.getViewColumns());
            setFilters(state.getFilters());
            setCustomColumns(state.getCustomColumns());
            setFlat(state.isFlat());
        }

        public ClientTableState getAppendTo() {
            return appendTo;
        }

        public void setAppendTo(ClientTableState appendTo) {
            this.appendTo = appendTo;
        }

        public boolean hasSorts() {
            return getSorts() != null && !getSorts().isEmpty();
        }

        public boolean hasConditions() {
            return getConditions() != null && !getConditions().isEmpty();
        }

        public boolean hasFilters() {
            return getFilters() != null && !getFilters().isEmpty();
        }

        public boolean hasCustomColumns() {
            return getCustomColumns() != null && !getCustomColumns().isEmpty();
        }

        public boolean hasViewColumns() {
            return getViewColumns() != null && !getViewColumns().isEmpty();
        }

        public boolean hasDropColumns() {
            return getDropColumns() != null && !getDropColumns().isEmpty();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            if (!super.equals(o))
                return false;

            final BatchOp batchOp = (BatchOp) o;

            // even if both have null handles, they should not be equal unless they are the same instance...
            if (source != null ? !source.equals(batchOp.source) : batchOp.source != null)
                return false;
            return target != null ? target.equals(batchOp.target) : batchOp.target == null;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (source != null ? source.hashCode() : 0);
            result = 31 * result + (target != null ? target.hashCode() : 0);
            return result;
        }

        public boolean isEqual(ClientTableState value) {
            // Array properties where order is important; properties are not commutative
            if (!getSorts().equals(value.getSorts())) {
                return false;
            }
            if (!getCustomColumns().equals(value.getCustomColumns())) {
                return false;
            }
            if (!getConditions().equals(value.getConditions())) {
                return false;
            }
            // Array properties where order is not important; properties are commutative
            if (getFilters().size() != value.getFilters().size() || !getFilters().containsAll(value.getFilters())) {
                return false;
            }
            if (getDropColumns().size() != value.getDropColumns().size()
                    || !getDropColumns().containsAll(value.getDropColumns())) {
                return false;
            }
            if (getViewColumns().size() != value.getViewColumns().size()
                    || !getViewColumns().containsAll(value.getViewColumns())) {
                return false;
            }

            // Other properties
            if (isFlat() != value.isFlat()) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "BatchOp{" +
            // "handles=" + handles +
                    ", state=" + (state == null ? null : state.toStringMinimal()) +
                    ", appendTo=" + (appendTo == null ? null : appendTo.toStringMinimal()) +
                    ", " + super.toString() + "}";
        }
    }

    public List<BatchTableRequest.Operation> serializable() {
        List<BatchTableRequest.Operation> send = new ArrayList<>();
        for (BatchOp op : ops) {
            if (!op.hasHandles()) {
                assert op.getState().isRunning() : "Only running states should be found in batch without a new handle";
                continue;
            }
            if (op.getState().isEmpty()) {
                op.getState().setResolution(ClientTableState.ResolutionState.FAILED,
                        "Table state abandoned before request was made");
                continue;
            }

            // Each BatchOp is assumed to have one source table and a list of specific ordered operations to produce
            // a target. The first operation always references its source by ticket (not batchOffset), and subsequent
            // operations within the same BatchOp chain each other via batchOffset. Because each BatchOp independently
            // references an exported source ticket, internalOffset correctly restarts at -1 for each BatchOp — the
            // batchOffset values are only used within a single chain, and the intermediate result between chains is
            // exported with its own ticket (see maybeInsertInterimTable / insertOp in RequestBatcher).
            Supplier<TableReference> prevTableSupplier = new Supplier<>() {
                // initialize as -1 because a reference to the "first" will be zero
                int internalOffset = -1;

                @Override
                public TableReference get() {
                    TableReference.Builder ref = TableReference.newBuilder();
                    if (internalOffset == -1) {
                        // for a given BatchOp, the first pb op references a table by ticket
                        ref.setTicket(op.getSource().makeTicket());
                    } else {
                        // every subsequent pb op references the entry before it
                        ref.setBatchOffset(internalOffset);
                    }
                    internalOffset++;

                    return ref.build();
                }
            };
            List<Function<Ticket, BatchTableRequest.Operation>> operations = Stream.of(
                    buildCustomColumns(op, prevTableSupplier),
                    buildViewColumns(op, prevTableSupplier),
                    buildFilter(op, prevTableSupplier),
                    buildSort(op, prevTableSupplier),
                    buildDropColumns(op, prevTableSupplier),
                    flattenOperation(op, prevTableSupplier))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (!operations.isEmpty()) {
                for (int i = 0; i < operations.size(); i++) {
                    Ticket resultTicket = op.getNewId().getTicket();
                    send.add(operations.get(i).apply(i == operations.size() - 1 ? resultTicket : null));
                }
            }
        }
        return send;
    }

    private Function<Ticket, BatchTableRequest.Operation> buildCustomColumns(BatchOp op,
            Supplier<TableReference> prevTableSupplier) {
        boolean match = false;
        for (CustomColumnDescriptor customColumn : op.getCustomColumns()) {
            if (op.getAppendTo() == null || !op.getAppendTo().hasCustomColumn(customColumn)) {
                match = true;
                break;
            }
        }
        if (!match) {
            return null;
        }

        return resultTicket -> {
            SelectOrUpdateRequest.Builder value = SelectOrUpdateRequest.newBuilder();

            for (CustomColumnDescriptor customColumn : op.getCustomColumns()) {
                if (op.getAppendTo() == null || !op.getAppendTo().hasCustomColumn(customColumn)) {
                    value.addColumnSpecs(customColumn.getExpression());
                }
            }

            value.setSourceId(prevTableSupplier.get());
            if (resultTicket != null) {
                value.setResultId(resultTicket);
            }

            return BatchTableRequest.Operation.newBuilder()
                    .setUpdateView(value)
                    .build();
        };
    }

    private Function<Ticket, BatchTableRequest.Operation> flattenOperation(BatchOp op,
            Supplier<TableReference> prevTableSupplier) {
        if (!op.isFlat()) {
            return null;
        }
        return resultTicket -> {
            FlattenRequest.Builder flatten = FlattenRequest.newBuilder().setSourceId(prevTableSupplier.get());
            if (resultTicket != null) {
                flatten.setResultId(resultTicket);
            }
            return BatchTableRequest.Operation.newBuilder()
                    .setFlatten(flatten)
                    .build();
        };
    }


    private Function<Ticket, BatchTableRequest.Operation> buildSort(BatchOp op,
            Supplier<TableReference> prevTableSupplier) {
        boolean match = false;
        for (Sort sort : op.getSorts()) {
            if (op.getAppendTo() == null || !op.getAppendTo().hasSort(sort)) {
                match = true;
                break;
            }
        }
        if (!match) {
            return null;
        }
        return resultTicket -> {
            SortTableRequest.Builder value = SortTableRequest.newBuilder();
            for (Sort sort : op.getSorts()) {
                if (op.getAppendTo() == null || !op.getAppendTo().hasSort(sort)) {
                    value.addSorts(sort.makeDescriptor());
                }
            }
            value.setSourceId(prevTableSupplier.get());
            if (resultTicket != null) {
                value.setResultId(resultTicket);
            }

            return BatchTableRequest.Operation.newBuilder()
                    .setSort(value)
                    .build();
        };
    }

    private Function<Ticket, BatchTableRequest.Operation> buildFilter(BatchOp op,
            Supplier<TableReference> prevTableSupplier) {
        boolean match = false;
        for (FilterCondition filter : op.getFilters()) {
            if (op.getAppendTo() == null || !op.getAppendTo().hasFilter(filter)) {
                match = true;
            }
        }
        if (!match) {
            return null;
        }
        return resultTicket -> {
            FilterTableRequest.Builder value = FilterTableRequest.newBuilder();
            for (FilterCondition filter : op.getFilters()) {
                if (op.getAppendTo() == null || !op.getAppendTo().hasFilter(filter)) {
                    value.addFilters(filter.makeDescriptor());
                }
            }

            value.setSourceId(prevTableSupplier.get());
            if (resultTicket != null) {
                value.setResultId(resultTicket);
            }

            return BatchTableRequest.Operation.newBuilder()
                    .setFilter(value)
                    .build();
        };
    }

    private Function<Ticket, BatchTableRequest.Operation> buildDropColumns(BatchOp op,
            Supplier<TableReference> prevTableSupplier) {
        if (op.getDropColumns().isEmpty()) {
            return null;
        }
        return resultTicket -> {
            DropColumnsRequest.Builder value = DropColumnsRequest.newBuilder();
            value.addAllColumnNames(op.getDropColumns());

            value.setSourceId(prevTableSupplier.get());
            if (resultTicket != null) {
                value.setResultId(resultTicket);
            }

            return BatchTableRequest.Operation.newBuilder()
                    .setDropColumns(value)
                    .build();
        };
    }

    private Function<Ticket, BatchTableRequest.Operation> buildViewColumns(BatchOp op,
            Supplier<TableReference> prevTableSupplier) {
        if (op.getViewColumns().isEmpty()) {
            return null;
        }
        return resultTicket -> {
            SelectOrUpdateRequest.Builder value = SelectOrUpdateRequest.newBuilder();
            for (String viewColumn : op.getViewColumns()) {
                value.addColumnSpecs(viewColumn);
            }

            value.setSourceId(prevTableSupplier.get());
            if (resultTicket != null) {
                value.setResultId(resultTicket);
            }

            return BatchTableRequest.Operation.newBuilder()
                    .setView(value)
                    .build();
        };
    }

    public BatchOp getOp() {
        if (next == null) {
            next = new BatchOp();
            ops.add(next);
        }
        return next;
    }

    public MappedIterable<BatchOp> getOps() {
        return MappedIterable.of(ops);
    }

    public void setSort(List<Sort> sorts) {
        getOp().setSorts(sorts);
    }

    public void setFilter(List<FilterCondition> filters) {
        getOp().setFilters(filters);
    }

    public void setCustomColumns(List<CustomColumnDescriptor> customColumns) {
        getOp().setCustomColumns(customColumns);
    }

    public void setDropColumns(List<String> dropColumns) {
        getOp().setDropColumns(dropColumns);
    }

    public void setViewColumns(List<String> viewColumns) {
        getOp().setViewColumns(viewColumns);
    }

    public void setFlat(boolean flat) {
        getOp().setFlat(flat);
    }

    public void doNextOp(BatchOp op) {
        if (!ops.contains(op)) {
            ops.add(op);
        }
        if (next == op) {
            next = null;
        }
    }

    public BatchOp getFirstOp() {
        assert !ops.isEmpty() : "Don't call getFirstOp on an empty batch!";
        return ops.get(0);
    }

    @Override
    public String toString() {
        return "BatchBuilder{" +
                "ops=" + ops +
                '}';
    }

    public String toStringMinimal() {
        StringBuilder b = new StringBuilder("BatchBuilder{ops=[");
        for (BatchOp op : ops) {
            b.append(op.toString());
        }
        return b.append("]}").toString();
    }

    public void clear() {
        ops.clear();
        next = null;
    }
}
