package io.deephaven.web.shared.batch;

import io.deephaven.web.shared.data.*;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A place to collect up table operations that can be batched. There are two ways to group operations, a single step can
 * perform more than one operation, and multiple steps can be specified. Within a given step, the operations will be run
 * in the default order and will only be exported to the client once, but by providing multiple steps any preferred
 * order can be achieve, or intermediate tables can be exported to the client.
 *
 * This object is only meant for serialization; all brains to construct it are in the RequestBatcher class.
 */
public class BatchTableRequest implements Serializable {

    private SerializedTableOps[] ops;

    public static class SerializedTableOps implements Serializable {

        private String[] dropColumns;
        private String[] viewColumns;
        private HandleMapping handles;
        private SortDescriptor[] sorts;
        private String[] conditions;
        private FilterDescriptor[] filters;
        private CustomColumnDescriptor[] customColumns;
        private HeadOrTailDescriptor headOrTail;
        private boolean isFlat;
        @Deprecated // deprecated for core#80, should be deleted in core#187
        private int updateIntervalMs;

        public String[] getDropColumns() {
            return dropColumns;
        }

        public void setDropColumns(String[] dropColumns) {
            this.dropColumns = dropColumns;
        }

        public String[] getViewColumns() {
            return viewColumns;
        }

        public void setViewColumns(String[] viewColumns) {
            this.viewColumns = viewColumns;
        }

        public SortDescriptor[] getSorts() {
            return sorts;
        }

        public void setSorts(SortDescriptor[] sorts) {
            this.sorts = sorts;
        }

        public FilterDescriptor[] getFilters() {
            return filters;
        }

        public void setFilters(FilterDescriptor[] filters) {
            this.filters = filters;
        }

        public String[] getConditions() {
            return conditions;
        }

        public void setConditions(String[] conditions) {
            this.conditions = conditions;
        }

        public CustomColumnDescriptor[] getCustomColumns() {
            return customColumns;
        }

        public void setCustomColumns(CustomColumnDescriptor[] customColumns) {
            this.customColumns = customColumns;
        }

        public HeadOrTailDescriptor getHeadOrTail() {
            return headOrTail;
        }

        public void setHeadOrTail(HeadOrTailDescriptor headOrTail) {
            this.headOrTail = headOrTail;
        }

        public HandleMapping getHandles() {
            return handles;
        }

        public void setHandles(HandleMapping handles) {
            this.handles = handles;
        }

        public void setFlat(boolean isFlat) {
            this.isFlat = isFlat;
        }

        @Deprecated
        public int getUpdateIntervalMs() {
            return updateIntervalMs;
        }

        @Deprecated
        public void setUpdateIntervalMs(int updateIntervalMs) {
            this.updateIntervalMs = updateIntervalMs;
        }

        @Override
        public String toString() {
            return "SerializedTableOps{" +
                    "handles=" + handles +
                    ", viewColumns=" + Arrays.toString(viewColumns) +
                    ", dropColumns=" + Arrays.toString(dropColumns) +
                    ", headOrTail=" + headOrTail +
                    ", sorts=" + Arrays.toString(sorts) +
                    ", filters=" + Arrays.toString(filters) +
                    ", customColumns=" + Arrays.toString(customColumns) +
                    ", isFlat=" + isFlat +
                    ", updateIntervalMs=" + updateIntervalMs +
                    '}';
        }

        public boolean hasDropColumns() {
            return dropColumns != null && dropColumns.length > 0;
        }

        public boolean hasViewColumns() {
            return viewColumns != null && viewColumns.length > 0;
        }

        public boolean hasCustomColumns() {
            return customColumns != null && customColumns.length > 0;
        }

        public boolean hasHeadOrTail() {
            return headOrTail != null;
        }

        /**
         * @return true any time we expect to need to send a full table definition back to client (columns added or
         *         removed, etc).
         */
        public boolean hasStructuralModification() {
            return hasCustomColumns();
        }

        public boolean hasSizeModification() {
            return hasFilters();
        }

        public boolean hasSorts() {
            return sorts != null && sorts.length > 0;
        }

        public boolean hasFilters() {
            return filters != null && filters.length > 0;
        }

        public boolean hasConditions() {
            return conditions != null && conditions.length > 0;
        }

        public boolean isFlat() {
            // serves as a getter _and_ a has-changes
            return isFlat;
        }

        public boolean hasUpdateIntervalMs() {
            return updateIntervalMs != 0;
        }

        public boolean isEmpty() {
            return !(hasDropColumns() || hasViewColumns() || hasHeadOrTail()
                    || hasSorts() || hasFilters() || hasCustomColumns() || isFlat() || hasUpdateIntervalMs());
        }
    }

    public SerializedTableOps[] getOps() {
        return ops;
    }

    public void setOps(SerializedTableOps[] ops) {
        this.ops = ops;
    }

    @Override
    public String toString() {
        return "BatchTableRequest{" +
                "ops=" + Arrays.toString(ops) +
                '}';
    }

    public boolean isEmpty() {
        return ops == null || ops.length == 0 || (ops.length == 1 && ops[0].getHandles() == null);
    }

}
