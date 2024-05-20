//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;

/**
 * Per-TableLocation, per-column key, state, and factory object.
 */
public interface ColumnLocation extends StringUtils.StringKeyedObject, NamedImplementation, LogOutputAppendable {

    /**
     * Get the {@link TableLocation} enclosing this ColumnLocation.
     *
     * @return the {@link TableLocation} enclosing this ColumnLocation
     */
    @NotNull
    TableLocation getTableLocation();

    /**
     * Get the column name for this ColumnLocation.
     *
     * @return the column name for this ColumnLocation
     */
    @NotNull
    String getName();

    /**
     * Check for existence of this ColumnLocation.
     *
     * @return True iff the ColumnLocation actually exists
     */
    boolean exists();

    /**
     * Get this column location cast to the specified type
     *
     * @return {@code this}, with the appropriate cast applied
     */
    default <CL extends ColumnLocation> CL cast() {
        // noinspection unchecked
        return (CL) this;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // ColumnRegion Factories
    // ------------------------------------------------------------------------------------------------------------------

    /**
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return A {@link ColumnRegionChar} for reading data from this ColumnLocation
     * @throws UnsupportedOperationException If this ColumnLocation does not contain char data
     */
    ColumnRegionChar<Values> makeColumnRegionChar(@NotNull ColumnDefinition<?> columnDefinition);

    /**
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return A {@link ColumnRegionByte} for reading data from this ColumnLocation
     * @throws UnsupportedOperationException If this ColumnLocation does not contain byte data
     */
    ColumnRegionByte<Values> makeColumnRegionByte(@NotNull ColumnDefinition<?> columnDefinition);

    /**
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return A {@link ColumnRegionShort} for reading data from this ColumnLocation
     * @throws UnsupportedOperationException If this ColumnLocation does not contain short data
     */
    ColumnRegionShort<Values> makeColumnRegionShort(@NotNull ColumnDefinition<?> columnDefinition);

    /**
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return A {@link ColumnRegionInt} for reading data from this ColumnLocation
     * @throws UnsupportedOperationException If this ColumnLocation does not contain int data
     */
    ColumnRegionInt<Values> makeColumnRegionInt(@NotNull ColumnDefinition<?> columnDefinition);

    /**
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return A {@link ColumnRegionLong} for reading data from this ColumnLocation
     * @throws UnsupportedOperationException If this ColumnLocation does not contain long data
     */
    ColumnRegionLong<Values> makeColumnRegionLong(@NotNull ColumnDefinition<?> columnDefinition);

    /**
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return A {@link ColumnRegionFloat} for reading data from this ColumnLocation
     * @throws UnsupportedOperationException If this ColumnLocation does not contain float data
     */
    ColumnRegionFloat<Values> makeColumnRegionFloat(@NotNull ColumnDefinition<?> columnDefinition);

    /**
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return A {@link ColumnRegionDouble} for reading data from this ColumnLocation
     * @throws UnsupportedOperationException If this ColumnLocation does not contain double data
     */
    ColumnRegionDouble<Values> makeColumnRegionDouble(@NotNull ColumnDefinition<?> columnDefinition);

    /**
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return A {@link ColumnRegionObject} for reading data from this ColumnLocation
     * @throws UnsupportedOperationException If this ColumnLocation does not contain object data
     */
    <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(@NotNull ColumnDefinition<TYPE> columnDefinition);

    // ------------------------------------------------------------------------------------------------------------------
    // StringKeyedObject implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    default String getStringRepresentation() {
        return getName();
    }

    // ------------------------------------------------------------------------------------------------------------------
    // LogOutputAppendable implementation / toString() override helper
    // ------------------------------------------------------------------------------------------------------------------

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
