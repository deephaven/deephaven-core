//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.testutil.sources.ImmutableColumnHolder;

import java.time.Instant;
import java.util.Arrays;
import java.util.Random;

public class ColumnInfo<T, U> {
    final Class<T> type;
    final Class<U> dataType;
    final Class<?> componentType;
    final TestDataGenerator<T, U> generator;
    final String name;
    final boolean immutable;
    final boolean indexed;

    final static ColAttributes[] ZERO_LENGTH_COLUMN_ATTRIBUTES_ARRAY = new ColAttributes[0];

    public enum ColAttributes {
        /**
         * This attribute has no effect.
         */
        None,
        /**
         * This attribute indicates that the column is immutable.
         */
        Immutable,
        /**
         * This attribute indicates that the column should be indexed. Only use this when enclosed by a
         * {@link io.deephaven.engine.liveness.LivenessScope} that was constructed with
         * {@code enforceStrongReachability == true}.
         */
        Indexed

    }

    public ColumnInfo(TestDataGenerator<T, U> generator, String name, ColAttributes... colAttributes) {
        this.dataType = generator.getType();
        this.type = generator.getColumnType();
        this.componentType = type.getComponentType();
        this.generator = generator;
        this.name = name;
        this.immutable = Arrays.asList(colAttributes).contains(ColAttributes.Immutable);
        this.indexed = Arrays.asList(colAttributes).contains(ColAttributes.Indexed);
    }

    public ColumnHolder<?> generateInitialColumn(RowSet rowSet, Random random) {
        final Chunk<Values> initialData = generator.populateChunk(rowSet, random);

        if (dataType == Long.class && type == Instant.class) {
            Require.eqFalse(immutable, "immutable");
            Require.eqFalse(indexed, "indexed");
            return ColumnHolder.getInstantColumnHolder(name, false, initialData);
        }

        if (immutable) {
            return new ImmutableColumnHolder<>(name, type, componentType, indexed, initialData);
        } else if (indexed) {
            return TstUtils.indexedColumnHolderForChunk(name, type, componentType, initialData);
        } else {
            return TstUtils.columnHolderForChunk(name, type, componentType, initialData);
        }
    }

    public void remove(RowSet rowKeys) {
        generator.onRemove(rowKeys);
    }

    public void shift(long start, long end, long delta) {
        generator.shift(start, end, delta);
    }

    public ColumnHolder<T> generateUpdateColumnHolder(RowSet keysToModify, Random random) {
        final Chunk<Values> chunk = generator.populateChunk(keysToModify, random);
        if (indexed) {
            return TstUtils.indexedColumnHolderForChunk(name, type, componentType, chunk);
        } else {
            return TstUtils.columnHolderForChunk(name, type, componentType, chunk);
        }
    }
}
