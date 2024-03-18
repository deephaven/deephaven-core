//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;

import java.time.Instant;

/**
 * Defaulted interfaces for various immutable {@link ColumnSource} types, in order to avoid having defaults at higher
 * levels in the class hierarchy.
 */
public final class ImmutableColumnSourceGetDefaults {

    /**
     * Default interface for immutable Object {@link ColumnSource} implementations.
     */
    public interface ForObject<DATA_TYPE>
            extends ColumnSourceGetDefaults.ForObject<DATA_TYPE>, ImmutableColumnSource<DATA_TYPE> {
    }

    /**
     * Default interface for immutable Boolean {@link ColumnSource} implementations.
     */
    public interface ForBoolean extends ColumnSourceGetDefaults.ForBoolean, ImmutableColumnSource<Boolean> {
    }

    /**
     * Default interface for immutable byte {@link ColumnSource} implementations.
     */
    public interface ForByte extends ColumnSourceGetDefaults.ForByte, ImmutableColumnSource<Byte> {
    }

    /**
     * Default interface for immutable char {@link ColumnSource} implementations.
     */
    public interface ForChar extends ColumnSourceGetDefaults.ForChar, ImmutableColumnSource<Character> {
    }

    /**
     * Default interface for immutable double {@link ColumnSource} implementations.
     */
    public interface ForDouble extends ColumnSourceGetDefaults.ForDouble, ImmutableColumnSource<Double> {
    }

    /**
     * Default interface for immutable float {@link ColumnSource} implementations.
     */
    public interface ForFloat extends ColumnSourceGetDefaults.ForFloat, ImmutableColumnSource<Float> {
    }

    /**
     * Default interface for immutable int {@link ColumnSource} implementations.
     */
    public interface ForInt extends ColumnSourceGetDefaults.ForInt, ImmutableColumnSource<Integer> {
    }

    /**
     * Default interface for immutable long-backed {@link ColumnSource} implementations.
     */
    public interface LongBacked<DATA_TYPE>
            extends ColumnSourceGetDefaults.LongBacked<DATA_TYPE>, ImmutableColumnSource<DATA_TYPE> {
    }

    /**
     * Default interface for immutable long {@link ColumnSource} implementations.
     */
    public interface ForLong extends ColumnSourceGetDefaults.ForLong, ImmutableColumnSource<Long> {
    }

    /**
     * Default interface for immutable {@link Instant} {@link ColumnSource} implementations.
     */
    public interface ForLongAsInstant
            extends ColumnSourceGetDefaults.ForLongAsInstant, ImmutableColumnSource<Instant> {
    }

    /**
     * Default interface for immutable short {@link ColumnSource} implementations.
     */
    public interface ForShort extends ColumnSourceGetDefaults.ForShort, ImmutableColumnSource<Short> {
    }
}
