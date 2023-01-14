package io.deephaven.engine.testutil.generator;

import io.deephaven.util.QueryConstants;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ByteAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2CharAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2CharOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2FloatAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ShortAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Static functions to produce a suitable shadowed fastutil map for a given type.
 */
public class GeneratorCollectionFactory {
    /**
     * Produce an AVLTreeMap from Long to the specified type. Use Java primitives where possible, and use the Deephaven
     * null constants defined in {@link QueryConstants} as the default value when there is no entry.
     *
     * @param mapType the type of class to make a map for
     * @param <U> template parameter on the type
     * @return a SortedMap from long to the given type
     */
    @NotNull
    public static <U> SortedMap<Long, U> makeSortedMapForType(Class<U> mapType) {
        if (mapType == Character.class || mapType == char.class) {
            final Long2CharAVLTreeMap retMap = new Long2CharAVLTreeMap();
            retMap.defaultReturnValue(QueryConstants.NULL_CHAR);
            // noinspection unchecked
            return (SortedMap<Long, U>) retMap;
        }
        if (mapType == Byte.class || mapType == byte.class) {
            Long2ByteAVLTreeMap retMap = new Long2ByteAVLTreeMap();
            retMap.defaultReturnValue(QueryConstants.NULL_BYTE);
            // noinspection unchecked
            return (SortedMap<Long, U>) retMap;
        }
        if (mapType == Short.class || mapType == short.class) {
            Long2ShortAVLTreeMap retMap = new Long2ShortAVLTreeMap();
            retMap.defaultReturnValue(QueryConstants.NULL_SHORT);
            // noinspection unchecked
            return (SortedMap<Long, U>) retMap;
        }
        if (mapType == Integer.class || mapType == int.class) {
            Long2IntAVLTreeMap retMap = new Long2IntAVLTreeMap();
            retMap.defaultReturnValue(QueryConstants.NULL_INT);
            // noinspection unchecked
            return (SortedMap<Long, U>) retMap;
        }
        if (mapType == Long.class || mapType == long.class) {
            Long2LongAVLTreeMap retMap = new Long2LongAVLTreeMap();
            retMap.defaultReturnValue(QueryConstants.NULL_LONG);
            // noinspection unchecked
            return (SortedMap<Long, U>) retMap;
        }
        if (mapType == Float.class || mapType == float.class) {
            Long2FloatAVLTreeMap retMap = new Long2FloatAVLTreeMap();
            retMap.defaultReturnValue(QueryConstants.NULL_FLOAT);
            // noinspection unchecked
            return (SortedMap<Long, U>) retMap;
        }
        if (mapType == Double.class || mapType == double.class) {
            Long2DoubleAVLTreeMap retMap = new Long2DoubleAVLTreeMap();
            retMap.defaultReturnValue(QueryConstants.NULL_DOUBLE);
            // noinspection unchecked
            return (SortedMap<Long, U>) retMap;
        }
        return new Long2ObjectAVLTreeMap<>();
    }

    /**
     * Produce an open addressed hash map from Long to the specified type. Use Java primitives where possible, and use
     * the Deephaven null constants defined in {@link QueryConstants} as the default value when there is no entry.
     *
     * @param mapType the type of class to make a map for
     * @param <U> template parameter on the type
     * @return a hash map from long to the given type
     */
    @NotNull
    public static <U> Map<Long, U> makeUnsortedMapForType(Class<U> mapType) {
        if (mapType == Character.class || mapType == char.class) {
            final Long2CharOpenHashMap retMap = new Long2CharOpenHashMap();
            retMap.defaultReturnValue(QueryConstants.NULL_CHAR);
            // noinspection unchecked
            return (Map<Long, U>) retMap;
        }
        if (mapType == Byte.class || mapType == byte.class) {
            Long2ByteOpenHashMap retMap = new Long2ByteOpenHashMap();
            retMap.defaultReturnValue(QueryConstants.NULL_BYTE);
            // noinspection unchecked
            return (Map<Long, U>) retMap;
        }
        if (mapType == Short.class || mapType == short.class) {
            Long2ShortOpenHashMap retMap = new Long2ShortOpenHashMap();
            retMap.defaultReturnValue(QueryConstants.NULL_SHORT);
            // noinspection unchecked
            return (Map<Long, U>) retMap;
        }
        if (mapType == Integer.class || mapType == int.class) {
            Long2IntOpenHashMap retMap = new Long2IntOpenHashMap();
            retMap.defaultReturnValue(QueryConstants.NULL_INT);
            // noinspection unchecked
            return (Map<Long, U>) retMap;
        }
        if (mapType == Long.class || mapType == long.class) {
            Long2LongOpenHashMap retMap = new Long2LongOpenHashMap();
            retMap.defaultReturnValue(QueryConstants.NULL_LONG);
            // noinspection unchecked
            return (Map<Long, U>) retMap;
        }
        if (mapType == Float.class || mapType == float.class) {
            Long2FloatOpenHashMap retMap = new Long2FloatOpenHashMap();
            retMap.defaultReturnValue(QueryConstants.NULL_FLOAT);
            // noinspection unchecked
            return (Map<Long, U>) retMap;
        }
        if (mapType == Double.class || mapType == double.class) {
            Long2DoubleOpenHashMap retMap = new Long2DoubleOpenHashMap();
            retMap.defaultReturnValue(QueryConstants.NULL_DOUBLE);
            // noinspection unchecked
            return (Map<Long, U>) retMap;
        }
        return new Long2ObjectOpenHashMap<>();
    }

    /**
     * Produce an array list of the specified type. Use Java primitives where possible.
     *
     * @param listType the type of class to make a list for
     * @param <U> template parameter on the type
     * @return a List of the given type
     */
    @NotNull
    public static <U> List<U> makeListForType(Class<U> listType) {
        if (listType == Character.class || listType == char.class) {
            final CharArrayList retMap = new CharArrayList();
            // noinspection unchecked
            return (List<U>) retMap;
        }
        if (listType == Byte.class || listType == byte.class) {
            ByteArrayList retMap = new ByteArrayList();
            // noinspection unchecked
            return (List<U>) retMap;
        }
        if (listType == Short.class || listType == short.class) {
            ShortArrayList retMap = new ShortArrayList();
            // noinspection unchecked
            return (List<U>) retMap;
        }
        if (listType == Integer.class || listType == int.class) {
            IntArrayList retMap = new IntArrayList();
            // noinspection unchecked
            return (List<U>) retMap;
        }
        if (listType == Long.class || listType == long.class) {
            LongArrayList retMap = new LongArrayList();
            // noinspection unchecked
            return (List<U>) retMap;
        }
        if (listType == Float.class || listType == float.class) {
            FloatArrayList retMap = new FloatArrayList();
            // noinspection unchecked
            return (List<U>) retMap;
        }
        if (listType == Double.class || listType == double.class) {
            DoubleArrayList retMap = new DoubleArrayList();
            // noinspection unchecked
            return (List<U>) retMap;
        }
        return new ArrayList<>();
    }
}
