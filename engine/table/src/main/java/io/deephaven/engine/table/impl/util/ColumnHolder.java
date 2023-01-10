/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.vector.ObjectVector;
import io.deephaven.time.DateTime;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.ColumnSource;

import java.lang.reflect.Array;
import java.util.Optional;

/**
 * Data to construct a data column.
 */
public class ColumnHolder<T> {
    public static final ColumnHolder[] ZERO_LENGTH_COLUMN_HOLDER_ARRAY = new ColumnHolder[0];

    /** The name of the column. */
    public final String name;
    /** The data type of the column. */
    public final Class<T> dataType;
    /** The data's component type of the column. */
    public final Class<?> componentType;
    /** Should the result column be grouped (used for test data only). */
    public final boolean grouped;

    private final Object arrayData;
    private final Chunk<Values> chunkData;

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param dataType column data type
     * @param componentType column component type (for array or {@link ObjectVector >} data types)
     * @param arrayData column data
     */
    @SuppressWarnings("unchecked")
    public ColumnHolder(String name, Class<T> dataType, Class<?> componentType, boolean grouped, T... arrayData) {
        this(name, grouped, dataType, componentType, arrayData);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param arrayData column data
     */
    public ColumnHolder(String name, boolean grouped, long... arrayData) {
        this(name, grouped, long.class, null, arrayData);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param arrayData column data
     */
    public ColumnHolder(String name, boolean grouped, int... arrayData) {
        this(name, grouped, int.class, null, arrayData);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param arrayData column data
     */
    public ColumnHolder(String name, boolean grouped, short... arrayData) {
        this(name, grouped, short.class, null, arrayData);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param arrayData column data
     */
    public ColumnHolder(String name, boolean grouped, char... arrayData) {
        this(name, grouped, char.class, null, arrayData);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param arrayData column data
     */
    public ColumnHolder(String name, boolean grouped, byte... arrayData) {
        this(name, grouped, byte.class, null, arrayData);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param arrayData column data
     */
    public ColumnHolder(String name, boolean grouped, double... arrayData) {
        this(name, grouped, double.class, null, arrayData);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param arrayData column data
     */
    public ColumnHolder(String name, boolean grouped, float... arrayData) {
        this(name, grouped, float.class, null, arrayData);
    }

    /**
     * Construct a new set of column data with a specified type. This overload allows the creation of a ColumnHolder
     * where the official data type type does not match the data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param dataType column data type
     * @param componentType column component type (for array or {@link ObjectVector >} data types)
     * @param arrayData column data
     */
    private ColumnHolder(String name, boolean grouped, Class<?> dataType, Class<?> componentType, Object arrayData) {
        if (!arrayData.getClass().isArray()) {
            throw new IllegalArgumentException("Data must be provided as an array");
        }
        if (!arrayData.getClass().getComponentType().isAssignableFrom(dataType)
                && !(dataType == DateTime.class && arrayData.getClass().getComponentType() == long.class)
                && !(dataType == Boolean.class && arrayData.getClass().getComponentType() == byte.class)) {
            throw new IllegalArgumentException(
                    "Incompatible data type: " + dataType + " can not be stored in array of type "
                            + arrayData.getClass());
        }
        this.name = NameValidator.validateColumnName(name);
        // noinspection unchecked
        this.dataType = (Class<T>) dataType;
        this.componentType = componentType;
        this.grouped = grouped;
        this.arrayData = arrayData;
        this.chunkData = null;
    }

    /**
     * Construct a new set of column data with a specified type using a chunk. This overload allows the creation of a
     * ColumnHolder where the official data type type does not match the data.
     *
     * @param name column name
     * @param type abstract data type for the column
     * @param grouped true if the column is grouped; false otherwise
     * @param chunkData column data
     */
    protected ColumnHolder(boolean chunkSentinel, String name, Class<?> dataType, Class<?> componentType,
            boolean grouped, Chunk<Values> chunkData) {
        // noinspection unchecked
        this.dataType = (Class<T>) dataType;
        this.componentType = componentType;
        this.arrayData = null;
        this.chunkData = chunkData;
        this.name = NameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    public static <T> ColumnHolder<T> makeForChunk(String name, Class<T> type, Class<?> componentType, boolean grouped,
            Chunk<Values> chunkData) {
        return new ColumnHolder<T>(false, name, type, componentType, grouped, chunkData);
    }


    /**
     * Create a column holder for a DateTime column where the values are represented as longs. Whatever process produces
     * a table from this column holder should respect this and create the appropriate type of ColumnSource. Under normal
     * conditions, this will be a DateTimeArraySource (see {@link #getColumnSource()}).
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data (long integers representing nanos since the epoch)
     * @return a DateTime column holder implemented with longs for storage
     */
    public static ColumnHolder<DateTime> getDateTimeColumnHolder(String name, boolean grouped, long... data) {
        return new ColumnHolder<>(name, grouped, DateTime.class, null, data);
    }

    /**
     * Create a column holder for a DateTime column where the values are represented as longs. Whatever process produces
     * a table from this column holder should respect this and create the appropriate type of ColumnSource. Under normal
     * conditions, this will be a DateTimeArraySource (see {@link #getColumnSource()}).
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param chunkData column data (long integers representing nanos since the epoch)
     * @return a DBDateTime column holder implemented with longs for storage
     */
    public static ColumnHolder<DateTime> getDateTimeColumnHolder(String name, boolean grouped,
            Chunk<Values> chunkData) {
        return new ColumnHolder<>(true, name, DateTime.class, null, grouped, chunkData);
    }

    /**
     * Create a column holder for a Boolean column where the calues are represented as bytes. The given byte array will
     * be converted to a Boolean array.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data (byte values where 1 represents true, 0 represents false, and null otherwise)
     * @return a Boolean column holder
     */
    public static ColumnHolder<Boolean> getBooleanColumnHolder(String name, boolean grouped, byte... data) {
        final Boolean[] dbData = new Boolean[data.length];
        for (int i = 0; i < data.length; i++) {
            if (data[i] == (byte) 0) {
                dbData[i] = false;
            } else if (data[i] == (byte) 1) {
                dbData[i] = true;
            } else {
                dbData[i] = null;
            }
        }
        return new ColumnHolder<>(name, Boolean.class, null, grouped, dbData);
    }

    /**
     * Create a column holder from an array object, inferring the data type from the given array object.
     *
     * @param name The column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data The data array
     * @return a column holder with a type matching the component type of the provided array
     */
    public static <T> ColumnHolder<T> createColumnHolder(String name, boolean grouped, T... data) {
        return new ColumnHolder(name, data.getClass().getComponentType(),
                data.getClass().getComponentType().getComponentType(), grouped, data);
    }

    public String getName() {
        return name;
    }

    /**
     * Gets a column source for the data. Other than the special case of DateTime columns, this requires that the type
     * specified match the component type of the actual data.
     *
     * @return column source constructed with data from this column holder
     */
    public ColumnSource<?> getColumnSource() {
        if (chunkData == null) {
            if (arrayData.getClass().getComponentType().equals(dataType)) {
                return ArrayBackedColumnSource.getMemoryColumnSourceUntyped(arrayData, dataType, componentType);
            } else if (dataType.equals(DateTime.class) && arrayData.getClass().getComponentType().equals(long.class)) {
                return ArrayBackedColumnSource.getDateTimeMemoryColumnSource((long[]) arrayData);
            } else {
                throw new IllegalStateException("Unsupported column holder data & type: " + dataType.getName() + ", "
                        + arrayData.getClass().getComponentType().getName());
            }
        }

        Assert.eqNull(arrayData, "arrayData");

        if (dataType.equals(DateTime.class) && chunkData.getChunkType() == ChunkType.Long) {
            return ArrayBackedColumnSource.getDateTimeMemoryColumnSource(chunkData.asLongChunk());
        }

        final ArrayBackedColumnSource<?> cs = ArrayBackedColumnSource.getMemoryColumnSource(
                chunkData.size(), dataType, componentType);
        try (final ChunkSink.FillFromContext ffc = cs.makeFillFromContext(chunkData.size())) {
            cs.fillFromChunk(ffc, chunkData, RowSequenceFactory.forRange(0, chunkData.size() - 1));
        }
        return cs;
    }

    public Optional<Object> getArrayData() {
        return Optional.ofNullable(arrayData);
    }

    public Optional<Chunk<Values>> getChunkData() {
        return Optional.ofNullable(chunkData);
    }

    public Chunk<Values> getChunk() {
        if (chunkData != null) {
            return chunkData;
        }
        if (arrayData == null) {
            return ObjectChunk.getEmptyChunk();
        }
        if (arrayData instanceof char[]) {
            return CharChunk.chunkWrap((char[]) arrayData);
        }
        if (arrayData instanceof byte[]) {
            return ByteChunk.chunkWrap((byte[]) arrayData);
        }
        if (arrayData instanceof short[]) {
            return ShortChunk.chunkWrap((short[]) arrayData);
        }
        if (arrayData instanceof int[]) {
            return IntChunk.chunkWrap((int[]) arrayData);
        }
        if (arrayData instanceof long[]) {
            return LongChunk.chunkWrap((long[]) arrayData);
        }
        if (arrayData instanceof float[]) {
            return FloatChunk.chunkWrap((float[]) arrayData);
        }
        if (arrayData instanceof double[]) {
            return DoubleChunk.chunkWrap((double[]) arrayData);
        }
        return ObjectChunk.chunkWrap((Object[]) arrayData);
    }

    public int size() {
        if (chunkData != null) {
            return chunkData.size();
        } else if (arrayData != null) {
            return Array.getLength(arrayData);
        } else {
            return 0;
        }
    }
}
