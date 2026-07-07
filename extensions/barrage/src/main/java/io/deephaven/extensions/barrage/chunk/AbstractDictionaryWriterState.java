//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import it.unimi.dsi.fastutil.bytes.Byte2IntOpenHashMap;
import it.unimi.dsi.fastutil.chars.Char2IntOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2IntOpenHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Shared map infrastructure for {@link LocalDictionaryWriterState} and {@link SharedDictionaryWriterState}.
 *
 * <p>
 * Exactly one map is allocated per instance, selected by {@code valuesChunkType}: a typed primitive map for primitive
 * columns, or an {@link Object2IntOpenHashMap} for {@link ChunkType#Object}. Subclasses supply one template method:
 * <ul>
 * <li>{@link #recordNewValue(Object)} — appends the new boxed value to the subclass's ordered list and returns the
 * 0-based index assigned to it</li>
 * </ul>
 */
abstract class AbstractDictionaryWriterState {

    @Nullable
    private final Object2IntOpenHashMap<Object> objectToIndex;
    @Nullable
    private final Byte2IntOpenHashMap byteToIndex;
    @Nullable
    private final Char2IntOpenHashMap charToIndex;
    @Nullable
    private final Short2IntOpenHashMap shortToIndex;
    @Nullable
    private final Int2IntOpenHashMap intToIndex;
    @Nullable
    private final Long2IntOpenHashMap longToIndex;
    @Nullable
    private final Float2IntOpenHashMap floatToIndex;
    @Nullable
    private final Double2IntOpenHashMap doubleToIndex;

    protected AbstractDictionaryWriterState(final ChunkType valuesChunkType) {
        Object2IntOpenHashMap<Object> objMap = null;
        Byte2IntOpenHashMap byteMap = null;
        Char2IntOpenHashMap charMap = null;
        Short2IntOpenHashMap shortMap = null;
        Int2IntOpenHashMap intMap = null;
        Long2IntOpenHashMap longMap = null;
        Float2IntOpenHashMap floatMap = null;
        Double2IntOpenHashMap doubleMap = null;

        switch (valuesChunkType) {
            case Byte:
                (byteMap = new Byte2IntOpenHashMap()).defaultReturnValue(-1);
                break;
            case Char:
                (charMap = new Char2IntOpenHashMap()).defaultReturnValue(-1);
                break;
            case Short:
                (shortMap = new Short2IntOpenHashMap()).defaultReturnValue(-1);
                break;
            case Int:
                (intMap = new Int2IntOpenHashMap()).defaultReturnValue(-1);
                break;
            case Long:
                (longMap = new Long2IntOpenHashMap()).defaultReturnValue(-1);
                break;
            case Float:
                (floatMap = new Float2IntOpenHashMap()).defaultReturnValue(-1);
                break;
            case Double:
                (doubleMap = new Double2IntOpenHashMap()).defaultReturnValue(-1);
                break;
            default:
                (objMap = new Object2IntOpenHashMap<>()).defaultReturnValue(-1);
                break;
        }

        this.objectToIndex = objMap;
        this.byteToIndex = byteMap;
        this.charToIndex = charMap;
        this.shortToIndex = shortMap;
        this.intToIndex = intMap;
        this.longToIndex = longMap;
        this.floatToIndex = floatMap;
        this.doubleToIndex = doubleMap;
    }

    /**
     * Appends a new value to the subclass's ordered list (e.g. {@code deltaValues} or {@code allValues}) and returns
     * the 0-based index assigned to it.
     */
    protected abstract int recordNewValue(@NotNull Object boxed);

    public int indexForObject(@NotNull final Object value) {
        final int existing = objectToIndex.getInt(value);
        if (existing != -1) {
            return existing;
        }
        final int index = recordNewValue(value);
        objectToIndex.put(value, index);
        return index;
    }

    public int indexForByte(final byte v) {
        final int existing = byteToIndex.get(v);
        if (existing != -1) {
            return existing;
        }
        final int index = recordNewValue(v);
        byteToIndex.put(v, index);
        return index;
    }

    public int indexForChar(final char v) {
        final int existing = charToIndex.get(v);
        if (existing != -1) {
            return existing;
        }
        final int index = recordNewValue(v);
        charToIndex.put(v, index);
        return index;
    }

    public int indexForShort(final short v) {
        final int existing = shortToIndex.get(v);
        if (existing != -1) {
            return existing;
        }
        final int index = recordNewValue(v);
        shortToIndex.put(v, index);
        return index;
    }

    public int indexForInt(final int v) {
        final int existing = intToIndex.get(v);
        if (existing != -1) {
            return existing;
        }
        final int index = recordNewValue(v);
        intToIndex.put(v, index);
        return index;
    }

    public int indexForLong(final long v) {
        final int existing = longToIndex.get(v);
        if (existing != -1) {
            return existing;
        }
        final int index = recordNewValue(v);
        longToIndex.put(v, index);
        return index;
    }

    public int indexForFloat(final float v) {
        // Canonicalize NaN so all bit patterns map to the same entry (fastutil uses floatToIntBits).
        final float key = Float.isNaN(v) ? Float.NaN : v;
        final int existing = floatToIndex.get(key);
        if (existing != -1) {
            return existing;
        }
        final int index = recordNewValue(key);
        floatToIndex.put(key, index);
        return index;
    }

    public int indexForDouble(final double v) {
        // Canonicalize NaN so all bit patterns map to the same entry (fastutil uses doubleToLongBits).
        final double key = Double.isNaN(v) ? Double.NaN : v;
        final int existing = doubleToIndex.get(key);
        if (existing != -1) {
            return existing;
        }
        final int index = recordNewValue(key);
        doubleToIndex.put(key, index);
        return index;
    }

    protected final void clearMaps() {
        if (objectToIndex != null)
            objectToIndex.clear();
        if (byteToIndex != null)
            byteToIndex.clear();
        if (charToIndex != null)
            charToIndex.clear();
        if (shortToIndex != null)
            shortToIndex.clear();
        if (intToIndex != null)
            intToIndex.clear();
        if (longToIndex != null)
            longToIndex.clear();
        if (floatToIndex != null)
            floatToIndex.clear();
        if (doubleToIndex != null)
            doubleToIndex.clear();
    }
}
