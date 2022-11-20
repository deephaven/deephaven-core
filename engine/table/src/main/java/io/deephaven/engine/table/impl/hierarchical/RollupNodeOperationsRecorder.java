package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.select.SelectColumn;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Stream;

/**
 * {@link RollupTable.NodeOperationsRecorder} implementation.
 */
class RollupNodeOperationsRecorder extends BaseNodeOperationsRecorder<RollupTable.NodeOperationsRecorder>
        implements RollupTable.NodeOperationsRecorder {

    private final RollupTable.NodeType nodeType;

    RollupNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final RollupTable.NodeType nodeType) {
        this(definition, nodeType, List.of(), List.of(), List.of());
    }

    private RollupNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final RollupTable.NodeType nodeType,
            @NotNull final Collection<? extends SelectColumn> recordedFormats,
            @NotNull final Collection<SortColumn> recordedSorts,
            @NotNull final Collection<? extends SelectColumn> recordedAbsoluteViews) {
        super(definition, recordedFormats, recordedSorts, recordedAbsoluteViews);
        this.nodeType = nodeType;
    }

    RollupTable.NodeType getNodeType() {
        return nodeType;
    }

    @Override
    RollupTable.NodeOperationsRecorder withFormats(@NotNull final Stream<? extends SelectColumn> formats) {
        return new RollupNodeOperationsRecorder(definition, nodeType,
                mergeFormats(getRecordedFormats().stream(), formats), getRecordedSorts(), getRecordedAbsoluteViews());
    }

    @Override
    RollupTable.NodeOperationsRecorder withSorts(
            @NotNull final Stream<SortColumn> sorts,
            @NotNull final Stream<? extends SelectColumn> absoluteViews) {
        // Arguably, we might want to look for duplicate absolute views, but that would imply that there were also
        // duplicative sorts, which we don't really expect to happen.
        return new RollupNodeOperationsRecorder(definition, nodeType, getRecordedFormats(),
                mergeSortColumns(getRecordedSorts().stream(), sorts),
                mergeAbsoluteViews(getRecordedAbsoluteViews().stream(), absoluteViews));
    }

    RollupNodeOperationsRecorder withOperations(@NotNull final RollupNodeOperationsRecorder other) {
        if (!definition.equals(other.definition) || nodeType != other.nodeType) {
            throw new IllegalArgumentException(
                    "Incompatible operation recorders; compatible recorders must be created from the same table, with the same node type");
        }
        return new RollupNodeOperationsRecorder(definition, nodeType,
                mergeFormats(getRecordedFormats().stream(), other.getRecordedFormats().stream()),
                mergeSortColumns(getRecordedSorts().stream(), other.getRecordedSorts().stream()),
                mergeAbsoluteViews(getRecordedAbsoluteViews().stream(), other.getRecordedAbsoluteViews().stream()));
    }
}
