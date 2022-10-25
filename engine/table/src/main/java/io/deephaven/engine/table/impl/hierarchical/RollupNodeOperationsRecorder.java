package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.select.SelectColumn;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
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
        this(definition, nodeType, List.of(), List.of());
    }

    private RollupNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final RollupTable.NodeType nodeType,
            @NotNull final Collection<? extends SelectColumn> recordedFormats,
            @NotNull final Collection<SortColumn> recordedSorts) {
        super(definition, recordedFormats, recordedSorts);
        this.nodeType = nodeType;
    }

    RollupTable.NodeType getNodeType() {
        return nodeType;
    }

    @Override
    RollupTable.NodeOperationsRecorder withFormats(@NotNull final Stream<? extends SelectColumn> formats) {
        return new RollupNodeOperationsRecorder(definition, nodeType,
                Stream.concat(recordedFormats.stream(), formats).collect(Collectors.toList()),
                recordedSorts);
    }

    @Override
    RollupTable.NodeOperationsRecorder withSorts(@NotNull final Stream<SortColumn> sorts) {
        return new RollupNodeOperationsRecorder(definition, nodeType, recordedFormats,
                Stream.concat(sorts, recordedSorts.stream()).collect(Collectors.toList()));
    }

    RollupNodeOperationsRecorder withOperations(@NotNull final RollupNodeOperationsRecorder other) {
        if (!definition.equals(other.definition) || nodeType != other.nodeType) {
            throw new IllegalArgumentException(
                    "Incompatible operation recorders; compatible recorders must be created from the same table, with the same node type");
        }
        return new RollupNodeOperationsRecorder(definition, nodeType,
                Stream.concat(recordedFormats.stream(), other.getRecordedFormats().stream())
                        .collect(Collectors.toList()),
                Stream.concat(other.getRecordedSorts().stream(), recordedSorts.stream()).collect(Collectors.toList()));
    }
}
