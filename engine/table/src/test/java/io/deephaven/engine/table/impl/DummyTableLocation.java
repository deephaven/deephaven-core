//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.SortColumn;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionByte;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionChar;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionDouble;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionFloat;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionInt;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionLong;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionObject;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionShort;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public final class DummyTableLocation extends AbstractTableLocation {

    public DummyTableLocation(@NotNull final TableKey tableKey,
            @NotNull final TableLocationKey tableLocationKey) {
        super(tableKey, tableLocationKey, false);
    }

    @Override
    public void refresh() {}

    @Override
    public @NotNull List<SortColumn> getSortedColumns() {
        return List.of();
    }

    @Override
    @NotNull
    public List<String[]> getDataIndexColumns() {
        return List.of();
    }

    @Override
    public boolean hasDataIndex(@NotNull final String... columns) {
        return false;
    }

    @NotNull
    @Override
    protected ColumnLocation makeColumnLocation(@NotNull final String name) {
        return new ColumnLocation() {
            @NotNull
            @Override
            public TableLocation getTableLocation() {
                return DummyTableLocation.this;
            }

            @NotNull
            @Override
            public String getName() {
                return name;
            }

            @Override
            public boolean exists() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnRegionChar<Values> makeColumnRegionChar(
                    @NotNull final ColumnDefinition<?> columnDefinition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnRegionByte<Values> makeColumnRegionByte(
                    @NotNull final ColumnDefinition<?> columnDefinition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnRegionShort<Values> makeColumnRegionShort(
                    @NotNull final ColumnDefinition<?> columnDefinition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnRegionInt<Values> makeColumnRegionInt(
                    @NotNull final ColumnDefinition<?> columnDefinition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnRegionLong<Values> makeColumnRegionLong(
                    @NotNull final ColumnDefinition<?> columnDefinition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnRegionFloat<Values> makeColumnRegionFloat(
                    @NotNull final ColumnDefinition<?> columnDefinition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnRegionDouble<Values> makeColumnRegionDouble(
                    @NotNull final ColumnDefinition<?> columnDefinition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(
                    @NotNull final ColumnDefinition<TYPE> columnDefinition) {
                throw new UnsupportedOperationException();
            }

        };
    }

    @Override
    @Nullable
    public BasicDataIndex loadDataIndex(@NotNull final String... columns) {
        throw new UnsupportedOperationException();
    }
}
