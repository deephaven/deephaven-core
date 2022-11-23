package io.deephaven.engine.testutil;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.generator.Generator;
import io.deephaven.engine.testutil.sources.ImmutableColumnHolder;
import io.deephaven.time.DateTime;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.TreeMap;

public class ColumnInfo<T, U> {
    final Class<T> type;
    final Class<U> dataType;
    final Generator<T, U> generator;
    final String name;
    final TreeMap<Long, U> data;
    final boolean immutable;
    final boolean grouped;

    final static ColAttributes[] ZERO_LENGTH_COLUMN_ATTRIBUTES_ARRAY = new ColAttributes[0];

    public enum ColAttributes {
        None, Immutable, Grouped
    }

    public ColumnInfo(Generator<T, U> generator, String name, ColAttributes... colAttributes) {
        this(generator.getType(), generator.getColumnType(), generator, name,
                Arrays.asList(colAttributes).contains(ColAttributes.Immutable),
                Arrays.asList(colAttributes).contains(ColAttributes.Grouped), new TreeMap<>());
    }

    private ColumnInfo(Class<U> dataType, Class<T> type, Generator<T, U> generator, String name, boolean immutable,
                       boolean grouped, TreeMap<Long, U> data) {
        this.dataType = dataType;
        this.type = type;
        this.generator = generator;
        this.name = name;
        this.data = data;
        this.immutable = immutable;
        this.grouped = grouped;
    }

    TreeMap<Long, U> populateMap(RowSet rowSet, Random random) {
        return generator.populateMap(data, rowSet, random);
    }

    @SuppressWarnings("unchecked")
    public ColumnHolder c() {
        if (dataType == Long.class && type == DateTime.class) {
            Require.eqFalse(immutable, "immutable");
            Require.eqFalse(grouped, "grouped");
            final long[] dataArray = data.values().stream().map(x -> (Long) x).mapToLong(x -> x).toArray();
            return ColumnHolder.getDateTimeColumnHolder(name, false, dataArray);
        }

        final U[] dataArray = data.values().toArray((U[]) Array.newInstance(dataType, data.size()));
        if (immutable) {
            return new ImmutableColumnHolder<>(name, dataType, null, grouped, dataArray);
        } else if (grouped) {
            return TstUtils.cG(name, dataArray);
        } else {
            return TstUtils.c(name, dataArray);
        }
    }

    public void remove(long key) {
        generator.onRemove(key, data.remove(key));
    }

    public void move(long from, long to) {
        final U movedValue = data.remove(from);
        generator.onMove(from, to, movedValue);
        data.put(to, movedValue);
    }

    public ColumnHolder populateMapAndC(RowSet keysToModify, Random random) {
        final Collection<U> newValues = populateMap(keysToModify, random).values();
        // noinspection unchecked
        final U[] valueArray = newValues.toArray((U[]) Array.newInstance(dataType, newValues.size()));
        if (grouped) {
            return TstUtils.cG(name, valueArray);
        } else {
            return TstUtils.c(name, valueArray);
        }
    }
}
