//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.ColumnSourceManager;
import io.deephaven.engine.table.impl.ColumnToCodecMappings;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory that assembles modular components for regioned source tables.
 */
public class RegionedTableComponentFactoryImpl implements RegionedTableComponentFactory {
    private static final Map<Class<?>, RegionedColumnSourceFactory> SIMPLE_DATA_TYPE_TO_REGIONED_COLUMN_SOURCE_SUPPLIER;

    static {
        Map<Class<?>, RegionedColumnSourceFactory> typeToSupplier = new HashMap<>();
        typeToSupplier.put(Byte.class, RegionedColumnSourceByte.AsValues::new);
        typeToSupplier.put(Character.class, RegionedColumnSourceChar.AsValues::new);
        typeToSupplier.put(Double.class, RegionedColumnSourceDouble.AsValues::new);
        typeToSupplier.put(Float.class, RegionedColumnSourceFloat.AsValues::new);
        typeToSupplier.put(Integer.class, RegionedColumnSourceInt.AsValues::new);
        typeToSupplier.put(Long.class, RegionedColumnSourceLong.AsValues::new);
        typeToSupplier.put(Short.class, RegionedColumnSourceShort.AsValues::new);
        typeToSupplier.put(Boolean.class, RegionedColumnSourceBoolean::new);
        typeToSupplier.put(Instant.class, RegionedColumnSourceInstant::new);
        SIMPLE_DATA_TYPE_TO_REGIONED_COLUMN_SOURCE_SUPPLIER = Collections.unmodifiableMap(typeToSupplier);
    }

    public static final RegionedTableComponentFactory INSTANCE = new RegionedTableComponentFactoryImpl();

    private RegionedTableComponentFactoryImpl() {}

    @Override
    public ColumnSourceManager createColumnSourceManager(
            final boolean isRefreshing,
            final boolean removeAllowed,
            @NotNull final ColumnToCodecMappings codecMappings,
            @NotNull final List<ColumnDefinition<?>> columnDefinitions) {
        return new RegionedColumnSourceManager(isRefreshing, removeAllowed, this, codecMappings, columnDefinitions);
    }

    /**
     * Create a new {@link RegionedColumnSource} appropriate to implement the supplied {@link ColumnDefinition}.
     *
     * @param manager The {@link RegionedColumnSourceManager} that will manage the new column source
     * @param columnDefinition The column definition
     * @param <DATA_TYPE> The data type of the column
     * @return A new RegionedColumnSource.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <DATA_TYPE> RegionedColumnSource<DATA_TYPE> createRegionedColumnSource(
            @NotNull final RegionedColumnSourceManager manager,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final ColumnToCodecMappings codecMappings) {
        Class<DATA_TYPE> dataType = (Class<DATA_TYPE>) TypeUtils.getBoxedType(columnDefinition.getDataType());

        if (columnDefinition.isPartitioning()) {
            return PartitioningSourceFactory.makePartitioningSource(manager, dataType);
        }

        final RegionedColumnSourceFactory rcsFactory =
                SIMPLE_DATA_TYPE_TO_REGIONED_COLUMN_SOURCE_SUPPLIER.get(dataType);
        if (rcsFactory != null) {
            return (RegionedColumnSource<DATA_TYPE>) rcsFactory.make(manager);
        }

        try {
            if (CharSequence.class.isAssignableFrom(dataType)) {
                return new RegionedColumnSourceWithDictionary<>(manager, dataType, null);
            } else {
                return new RegionedColumnSourceObject.AsValues<>(manager, dataType,
                        columnDefinition.getComponentType());
            }
        } catch (IllegalArgumentException except) {
            throw new TableDataException(
                    "Can't create column for " + dataType + " in column definition " + columnDefinition, except);
        }
    }
}
