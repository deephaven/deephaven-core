//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.locations;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.locations.impl.AbstractColumnLocation;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.generic.region.*;
import org.jetbrains.annotations.NotNull;

public final class TableBackedColumnLocation
        extends AbstractColumnLocation
        implements AppendOnlyRegionAccessor<Values> {

    private static final int PAGE_SIZE = 1 << 16;

    private final ColumnSource<?> columnSource;

    TableBackedColumnLocation(
            @NotNull final TableBackedTableLocation tableLocation,
            @NotNull final String name) {
        super(tableLocation, name);
        columnSource = tableLocation.table().getDefinition().getColumnNameSet().contains(name)
                ? ReinterpretUtils.maybeConvertToPrimitive(tableLocation.table().getColumnSource(name))
                : null;
    }

    @Override
    public boolean exists() {
        return columnSource != null;
    }

    @Override
    public ColumnRegionChar<Values> makeColumnRegionChar(@NotNull final ColumnDefinition<?> columnDefinition) {
        return new AppendOnlyFixedSizePageRegionChar<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
    }

    @Override
    public ColumnRegionByte<Values> makeColumnRegionByte(@NotNull final ColumnDefinition<?> columnDefinition) {
        return new AppendOnlyFixedSizePageRegionByte<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
    }

    @Override
    public ColumnRegionShort<Values> makeColumnRegionShort(@NotNull final ColumnDefinition<?> columnDefinition) {
        return new AppendOnlyFixedSizePageRegionShort<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
    }

    @Override
    public ColumnRegionInt<Values> makeColumnRegionInt(@NotNull final ColumnDefinition<?> columnDefinition) {
        return new AppendOnlyFixedSizePageRegionInt<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
    }

    @Override
    public ColumnRegionLong<Values> makeColumnRegionLong(@NotNull final ColumnDefinition<?> columnDefinition) {
        return new AppendOnlyFixedSizePageRegionLong<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
    }

    @Override
    public ColumnRegionFloat<Values> makeColumnRegionFloat(@NotNull final ColumnDefinition<?> columnDefinition) {
        return new AppendOnlyFixedSizePageRegionFloat<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
    }

    @Override
    public ColumnRegionDouble<Values> makeColumnRegionDouble(@NotNull final ColumnDefinition<?> columnDefinition) {
        return new AppendOnlyFixedSizePageRegionDouble<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
    }

    @Override
    public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(
            @NotNull final ColumnDefinition<TYPE> columnDefinition) {
        return new AppendOnlyFixedSizePageRegionObject<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
    }

    @Override
    public long size() {
        return getTableLocation().getSize();
    }

    @Override
    public void readChunkPage(
            final long firstRowPosition,
            final int minimumSize,
            @NotNull final WritableChunk<Values> destination) {
        // @formatter:off
        try (final ChunkSource.FillContext fillContext = columnSource.makeFillContext(minimumSize);
             final RowSequence rows =
                     RowSequenceFactory.forRange(firstRowPosition, firstRowPosition + minimumSize - 1)) {
            // @formatter:on
            columnSource.fillChunk(fillContext, destination, rows);
        }
    }
}
