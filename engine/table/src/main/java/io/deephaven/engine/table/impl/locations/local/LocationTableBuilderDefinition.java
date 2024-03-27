//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.util.PartitionParser;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link KeyValuePartitionLayout.LocationTableBuilder LocationTableBuilder} implementation that uses the a
 * {@link TableDefinition} and {@link PartitionParser PartitionParsers} to assemble location tables.
 */
public final class LocationTableBuilderDefinition implements KeyValuePartitionLayout.LocationTableBuilder {

    /**
     * The partitioning columns, mapped from name to state, in definition order.
     */
    private final Map<String, PartitioningColumn<?>> nameToPartitioningColumn;

    /**
     * The partitioning columns, in input order determined by registration.
     */
    private PartitioningColumn<?>[] partitioningColumnsInInputOrder;

    /**
     * Partitioning column information and accumulated data.
     */
    private static class PartitioningColumn<DATA_TYPE> {

        private final ColumnDefinition<DATA_TYPE> columnDefinition;

        private final PartitionParser parser;
        private final WritableColumnSource<DATA_TYPE> data;

        private PartitioningColumn(@NotNull final ColumnDefinition<DATA_TYPE> columnDefinition) {
            this.columnDefinition = columnDefinition;

            parser = PartitionParser.lookup(columnDefinition.getDataType(), columnDefinition.getComponentType());
            data = ArrayBackedColumnSource.getMemoryColumnSource(
                    columnDefinition.getDataType(), columnDefinition.getComponentType());
        }

        private void set(final int rowKey, @NotNull final String value) {
            data.ensureCapacity(rowKey + 1, false);
            // noinspection unchecked
            data.set(rowKey, (DATA_TYPE) parser.parse(value));
        }
    }

    /**
     * The count of locations accepted.
     */
    private int locationCount;

    public LocationTableBuilderDefinition(@NotNull final TableDefinition tableDefinition) {
        this.nameToPartitioningColumn = tableDefinition.getColumnStream()
                .sequential()
                .filter(cd -> cd.getColumnType() == ColumnDefinition.ColumnType.Partitioning)
                .collect(Collectors.toMap(
                        ColumnDefinition::getName, PartitioningColumn::new, Assert::neverInvoked, LinkedHashMap::new));
    }

    @Override
    public void registerPartitionKeys(@NotNull final Collection<String> partitionKeys) {
        if (partitioningColumnsInInputOrder != null) {
            throw new IllegalStateException("Partition keys already registered");
        }
        if (!Set.copyOf(partitionKeys).equals(nameToPartitioningColumn.keySet())) {
            throw new IllegalArgumentException(String.format(
                    "Partition keys mismatch: expected %s, encountered %s",
                    nameToPartitioningColumn.keySet(), partitionKeys));
        }
        partitioningColumnsInInputOrder = partitionKeys.stream()
                .map(nameToPartitioningColumn::get).toArray(PartitioningColumn[]::new);
    }

    @Override
    public void acceptLocation(@NotNull final Collection<String> partitionValueStrings) {
        if (partitioningColumnsInInputOrder == null) {
            throw new IllegalStateException("Partition keys not registered");
        }
        if (partitionValueStrings.size() != partitioningColumnsInInputOrder.length) {
            throw new IllegalArgumentException(String.format(
                    "Partition mismatch for location: partition keys %s, encountered partition values %s",
                    Arrays.stream(partitioningColumnsInInputOrder)
                            .map(pc -> pc.columnDefinition.getName()).collect(Collectors.toList()),
                    partitionValueStrings));
        }
        int pci = 0;
        for (final String partitionValueString : partitionValueStrings) {
            partitioningColumnsInInputOrder[pci++].set(locationCount, partitionValueString);
        }
        ++locationCount;
    }

    @Override
    public Table build() {
        return new QueryTable(
                RowSetFactory.flat(locationCount).toTracking(),
                nameToPartitioningColumn.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        (final Map.Entry<String, PartitioningColumn<?>> entry) -> entry.getValue().data,
                        Assert::neverInvoked,
                        LinkedHashMap::new)));
    }
}
