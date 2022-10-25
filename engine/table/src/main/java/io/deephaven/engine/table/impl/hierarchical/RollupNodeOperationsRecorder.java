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

    RollupNodeOperationsRecorder(@NotNull final TableDefinition definition) {
        this(definition, List.of(), List.of());
    }

    private RollupNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final Collection<? extends SelectColumn> recordedFormats,
            @NotNull final Collection<SortColumn> recordedSorts) {
        super(definition, recordedFormats, recordedSorts);
    }

    @Override
    RollupTable.NodeOperationsRecorder withFormats(@NotNull final Stream<? extends SelectColumn> formats) {
        return new RollupNodeOperationsRecorder(definition,
                Stream.concat(recordedFormats.stream(), formats).collect(Collectors.toList()),
                recordedSorts);
    }

    @Override
    RollupTable.NodeOperationsRecorder withSorts(@NotNull final Stream<SortColumn> sorts) {
        return new RollupNodeOperationsRecorder(definition, recordedFormats,
                Stream.concat(sorts, recordedSorts.stream()).collect(Collectors.toList()));
    }
}
