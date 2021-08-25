package io.deephaven.web.client.api.batch;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.batchtablerequest.Operation;
import io.deephaven.web.client.api.Sort;
import io.deephaven.web.client.api.TableTicket;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.CustomColumnDescriptor;
import io.deephaven.web.shared.fu.MappedIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
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

    public JsArray<Operation> serializable() {
        if (ops.isEmpty()) {
            return new JsArray<>();
        }

        JsArray<Operation> send = new JsArray<>();
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
            // a target. Intermediate items each use the offset before them
            Supplier<TableReference> prevTableSupplier = new Supplier<TableReference>() {
                // initialize as -1 because a reference to the "first" will be zero
                int internalOffset = -1;

                @Override
                public TableReference get() {
                    TableReference ref = new TableReference();
                    if (internalOffset == -1) {
                        // for a given BatchOp, the first pb op references a table by ticket
                        ref.setTicket(op.getSource().makeTicket());
                    } else {
                        // every subsequent pb op references the entry before it
                        ref.setBatchOffset(send.length + internalOffset);
                    }
                    internalOffset++;

                    return ref;
                }
            };
            Consumer<Ticket>[] lastOp = new Consumer[1];

            List<Operation> operations = Stream.of(
                    buildCustomColumns(op, prevTableSupplier, lastOp),
                    buildViewColumns(op, prevTableSupplier, lastOp),
                    buildFilter(op, prevTableSupplier, lastOp),
                    buildSort(op, prevTableSupplier, lastOp),
                    buildDropColumns(op, prevTableSupplier, lastOp),
                    flattenOperation(op, prevTableSupplier, lastOp))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            lastOp[0].accept(op.getNewId().makeTicket());

            // after building the entire collection, append to the set of steps we'll send
            for (int i = 0; i < operations.size(); i++) {
                send.push(operations.get(i));
            }

        }
        return send;
    }

    private Operation buildCustomColumns(BatchOp op, Supplier<TableReference> prevTableSupplier,
            Consumer<Ticket>[] lastOp) {
        SelectOrUpdateRequest value = new SelectOrUpdateRequest();

        for (CustomColumnDescriptor customColumn : op.getCustomColumns()) {
            if (op.getAppendTo() == null || !op.getAppendTo().hasCustomColumn(customColumn)) {
                value.addColumnSpecs(customColumn.getExpression());
            }
        }
        if (value.getColumnSpecsList().length == 0) {
            return null;
        }

        Operation updateViewOp = new Operation();
        updateViewOp.setUpdateView(value);

        value.setSourceId(prevTableSupplier.get());
        lastOp[0] = value::setResultId;

        return updateViewOp;
    }

    private Operation flattenOperation(BatchOp op, Supplier<TableReference> prevTableSupplier,
            Consumer<Ticket>[] lastOp) {
        if (!op.isFlat()) {
            return null;
        }

        Operation flattenOp = new Operation();
        FlattenRequest value = new FlattenRequest();
        flattenOp.setFlatten(value);
        value.setSourceId(prevTableSupplier.get());
        lastOp[0] = value::setResultId;
        return flattenOp;
    }

    private Operation buildSort(BatchOp op, Supplier<TableReference> prevTableSupplier, Consumer<Ticket>[] lastOp) {
        SortTableRequest value = new SortTableRequest();
        for (Sort sort : op.getSorts()) {
            if (op.getAppendTo() == null || !op.getAppendTo().hasSort(sort)) {
                value.addSorts(sort.makeDescriptor());
            }
        }
        if (value.getSortsList().length == 0) {
            return null;
        }
        Operation sortOp = new Operation();
        sortOp.setSort(value);

        value.setSourceId(prevTableSupplier.get());
        lastOp[0] = value::setResultId;

        return sortOp;
    }

    private Operation buildFilter(BatchOp op, Supplier<TableReference> prevTableSupplier, Consumer<Ticket>[] lastOp) {
        FilterTableRequest value = new FilterTableRequest();
        for (FilterCondition filter : op.getFilters()) {
            if (op.getAppendTo() == null || !op.getAppendTo().hasFilter(filter)) {
                value.addFilters(filter.makeDescriptor());
            }
        }
        if (value.getFiltersList().length == 0) {
            return null;
        }
        Operation filterOp = new Operation();
        filterOp.setFilter(value);

        value.setSourceId(prevTableSupplier.get());
        lastOp[0] = value::setResultId;

        return filterOp;
    }

    private Operation buildDropColumns(BatchOp op, Supplier<TableReference> prevTableSupplier,
            Consumer<Ticket>[] lastOp) {
        DropColumnsRequest value = new DropColumnsRequest();
        for (String dropColumn : op.getDropColumns()) {
            value.addColumnNames(dropColumn);
        }

        if (value.getColumnNamesList().length == 0) {
            return null;
        }
        Operation dropOp = new Operation();
        dropOp.setDropColumns(value);

        value.setSourceId(prevTableSupplier.get());
        lastOp[0] = value::setResultId;

        return dropOp;
    }

    private Operation buildViewColumns(BatchOp op, Supplier<TableReference> prevTableSupplier,
            Consumer<Ticket>[] lastOp) {
        SelectOrUpdateRequest value = new SelectOrUpdateRequest();
        for (String dropColumn : op.getDropColumns()) {
            value.addColumnSpecs(dropColumn);
        }
        if (value.getColumnSpecsList().length == 0) {
            return null;
        }

        Operation dropOp = new Operation();
        dropOp.setView(value);

        value.setSourceId(prevTableSupplier.get());
        lastOp[0] = value::setResultId;

        return dropOp;
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
