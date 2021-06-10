/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Per-TableLocation, per-column key and state object.
 */
public interface ColumnLocation<TLT extends TableLocation> extends StringUtils.StringKeyedObject, NamedImplementation, LogOutputAppendable {

    /**
     * Get the {@link TableLocation} enclosing this ColumnLocation.
     *
     * @return the {@link TableLocation} enclosing this ColumnLocation
     */
    @NotNull TLT getTableLocation();

    /**
     * Get the column name for this ColumnLocation.
     *
     * @return the column name for this ColumnLocation
     */
    @NotNull String getName();

    /**
     * Check for existence of this ColumnLocation.
     *
     * @return True iff the ColumnLocation actually exists
     */
    boolean exists();

    /**
     * <p>Get the metadata object stored with this column, or null if no such data exists.
     * <p>This is typically a value to range map (grouping metadata). The value to range map, if non-null, is a map from
     * unique (boxed) column values for this location to the associated ranges in which they occur.
     * Ranges are either 2-element int[]s, or 2-element long[]s.
     *
     * @return The metadata stored with this column, or null if no such data exists
     */
    @Nullable
    <METADATA_TYPE> METADATA_TYPE getMetadata(ColumnDefinition columnDefinition);

    /**
     * Get the format that was used to persist this column location.
     *
     * @return The format for this column location
     */
    @FinalDefault
    default TableLocation.Format getFormat() {
        return getTableLocation().getFormat();
    }

    //TODO: Add an enum type parameter to Format so we can downcast by format, rather than having per-format methods.

    /**
     * Get this column location cast to the {@link TableLocation.Format#PARQUET} format sub-interface.
     *
     * @return {@code this}, with the appropriate cast applied
     */
    default ParquetFormatColumnLocation<Attributes.Values, TLT> asParquetFormat() {
        return (ParquetFormatColumnLocation<Attributes.Values, TLT>) this;
    }

    //------------------------------------------------------------------------------------------------------------------
    // StringKeyedObject implementations
    //------------------------------------------------------------------------------------------------------------------

    @Override
    default String getStringRepresentation() {
        return getName();
    }

    //------------------------------------------------------------------------------------------------------------------
    // LogOutputAppendable implementation / toString() override helper
    //------------------------------------------------------------------------------------------------------------------

    @Override
    default LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getTableLocation())
                .append(':').append(getImplementationName())
                .append('[').append(getName())
                .append(']');
    }

    default String toStringHelper() {
        return getTableLocation().toString()
                + ':' + getImplementationName()
                + '[' + getName()
                + ']';
    }
}
