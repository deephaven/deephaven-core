/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.Pair;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.StringSetWrapper;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.utils.*;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.utils.*;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import junit.framework.AssertionFailedError;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

public class TstUtils {

    public static <T> ColumnHolder c(String name, T... data) {
        return TableTools.col(name, data);
    }

    public static Index i(long... keys) {
        return Index.FACTORY.getIndexByValues(keys);
    }

    public static Index ir(final long firstKey, final long lastKey) {
        return Index.FACTORY.getIndexByRange(firstKey, lastKey);
    }

    public static void addToTable(final Table table, final Index index, final ColumnHolder... columnHolders) {
        Require.requirement(table.isLive(), "table.isLive()");
        if (table instanceof DynamicTable) {
            Require.requirement(((DynamicTable)table).isRefreshing(), "table.isRefreshing()");
        }
        final Set<String> usedNames = new HashSet<>();
        for (ColumnHolder columnHolder : columnHolders) {
            if (!usedNames.add(columnHolder.name)) {
                throw new IllegalStateException("Added to the same column twice!");
            }
            final ColumnSource columnSource = table.getColumnSource(columnHolder.name);
            final Object[] boxedArray = ArrayUtils.getBoxedArray(columnHolder.data);
            final Index colIndex = (boxedArray.length == 0) ? TstUtils.i() : index;
            if (colIndex.size() != boxedArray.length) {
                throw new IllegalArgumentException(columnHolder.name + ": Invalid data addition: index=" + colIndex.size() + ", boxedArray=" + boxedArray.length);
            }

            if (colIndex.size() == 0) {
                continue;
            }

            if (columnSource instanceof DateTimeTreeMapSource && columnHolder.dataType == long.class) {
                final DateTimeTreeMapSource treeMapSource = (DateTimeTreeMapSource)columnSource;
                treeMapSource.add(colIndex, (Long[])boxedArray);
            }
            else if (columnSource.getType() != columnHolder.dataType) {
                throw new UnsupportedOperationException(columnHolder.name + ": Adding invalid type: source.getType()=" + columnSource.getType() + ", columnHolder=" + columnHolder.dataType);
            }

            if (columnSource instanceof TreeMapSource) {
                final TreeMapSource treeMapSource = (TreeMapSource)columnSource;
                //noinspection unchecked
                treeMapSource.add(colIndex, boxedArray);
            } else if (columnSource instanceof DateTimeTreeMapSource) {
                final DateTimeTreeMapSource treeMapSource = (DateTimeTreeMapSource)columnSource;
                treeMapSource.add(colIndex, (Long[])boxedArray);
            }
        }

        if (!usedNames.containsAll(table.getColumnSourceMap().keySet())) {
            final Set<String> expected = new LinkedHashSet<>(table.getColumnSourceMap().keySet());
            expected.removeAll(usedNames);
            throw new IllegalStateException("Not all columns were populated, missing " + expected);
        }

        table.getIndex().insert(index);
        if (table.isFlat()) {
            Assert.assertion(table.getIndex().isFlat(), "table.getIndex().isFlat()", table.getIndex(), "table.getIndex()", index, "index");
        }
    }

    public static void removeRows(Table table, Index index) {
        Require.requirement(table.isLive(), "table.isLive()");
        if (table instanceof DynamicTable) {
            Require.requirement(((DynamicTable)table).isRefreshing(), "table.isRefreshing()");
        }
        table.getIndex().remove(index);
        if (table.isFlat()) {
            Assert.assertion(table.getIndex().isFlat(), "table.getIndex().isFlat()", table.getIndex(), "table.getIndex()", index, "index");
        }
        for (ColumnSource columnSource : table.getColumnSources()) {
            if (columnSource instanceof TreeMapSource) {
                final TreeMapSource treeMapSource = (TreeMapSource)columnSource;
                treeMapSource.remove(index);
            }
        }
    }

    // TODO: this is just laziness, make it go away
    public static <T> ColumnHolder cG(String name, T... data) {
        return ColumnHolder.createColumnHolder(name, true, data);
    }

    public static ColumnHolder getRandomStringCol(String colName, int size, Random random) {
        final String[] data = new String[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = Long.toString(random.nextLong(), 'z' - 'a' + 10);
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomStringArrayCol(String colName, int size, Random random, int maxSz) {
        final String data[][] = new String[size][];
        for (int i = 0; i < data.length; i++) {
            final String[] v = new String[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = Long.toString(random.nextLong(), 'z' - 'a' + 10);
            }
            data[i] = v;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomStringSetCol(String colName, int size, Random random, int maxSz) {
        final StringSet data[] = new StringSet[size];
        for (int i = 0; i < data.length; i++) {
            final String[] v = new String[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = Long.toString(random.nextLong(), 'z' - 'a' + 10);
            }
            data[i] = new StringSetWrapper(Arrays.asList(v));
        }
        return c(colName, data);
    }

    public static Index getRandomIndex(long minValue, int size, Random random) {
        final Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();
        long previous = minValue;
        for (int i = 0; i < size; i++) {
            previous += (1 + random.nextInt(100));
            builder.addKey(previous);
        }
        return builder.getIndex();
    }

    public static ColumnHolder getRandomIntCol(String colName, int size, Random random) {
        final Integer[] data = new Integer[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt(1000);
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomDoubleCol(String colName, int size, Random random) {
        final Double[] data = new Double[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextDouble();
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomFloatCol(String colName, int size, Random random) {
        final float data[] = new float[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextFloat();
        }
        return new ColumnHolder(colName, false, data);
    }

    public static ColumnHolder getRandomShortCol(String colName, int size, Random random) {
        final short data[] = new short[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (short)random.nextInt(Short.MAX_VALUE);
        }
        return new ColumnHolder(colName, false, data);
    }

    public static ColumnHolder getRandomLongCol(String colName, int size, Random random) {
        final long data[] = new long[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextLong();
        }
        return new ColumnHolder(colName, false, data);
    }

    public static ColumnHolder getRandomBooleanCol(String colName, int size, Random random) {
        final Boolean data[] = new Boolean[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextBoolean();
        }
        return ColumnHolder.createColumnHolder(colName, false, data);
    }

    public static ColumnHolder getRandomCharCol(String colName, int size, Random random) {
        final char data[] = new char[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char)random.nextInt();
        }
        return new ColumnHolder(colName, false, data);
    }

    public static ColumnHolder getRandomByteCol(String colName, int size, Random random) {
        final byte data[] = new byte[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)random.nextInt();
        }
        return new ColumnHolder(colName, false, data);
    }

    public static ColumnHolder getRandomByteArrayCol(String colName, int size, Random random, int maxSz) {
        final byte data[][] = new byte[size][];
        for (int i = 0; i < size; i++) {
            final byte[] b = new byte[random.nextInt(maxSz)];
            random.nextBytes(b);
            data[i] = b;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomBooleanArrayCol(String colName, int size, Random random, int maxSz) {
        final Boolean data[][] = new Boolean[size][];
        for (int i = 0; i < size; i++) {
            final Boolean[] v = new Boolean[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextBoolean();
            }
            data[i] = v;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomIntArrayCol(String colName, int size, Random random, int maxSz) {
        final int data[][] = new int[size][];
        for (int i = 0; i < size; i++) {
            final int[] v = new int[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextInt();
            }
            data[i] = v;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomLongArrayCol(String colName, int size, Random random, int maxSz) {
        final long data[][] = new long[size][];
        for (int i = 0; i < size; i++) {
            final long[] v = new long[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextLong();
            }
            data[i] = v;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomShortArrayCol(String colName, int size, Random random, int maxSz) {
        final short data[][] = new short[size][];
        for (int i = 0; i < size; i++) {
            final short[] v = new short[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = (short)random.nextInt();
            }
            data[i] = v;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomDoubleArrayCol(String colName, int size, Random random, int maxSz) {
        final double data[][] = new double[size][];
        for (int i = 0; i < size; i++) {
            final double[] v = new double[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextDouble();
            }
            data[i] = v;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomFloatArrayCol(String colName, int size, Random random, int maxSz) {
        final float data[][] = new float[size][];
        for (int i = 0; i < size; i++) {
            final float[] v = new float[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextFloat();
            }
            data[i] = v;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomCharArrayCol(String colName, int size, Random random, int maxSz) {
        final char data[][] = new char[size][];
        for (int i = 0; i < size; i++) {
            final char[] v = new char[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = (char)random.nextInt();
            }
            data[i] = v;
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomBigDecimalCol(String colName, int size, Random random) {
        final BigDecimal[] data = new BigDecimal[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = BigDecimal.valueOf(random.nextDouble());
        }
        return c(colName, data);
    }

    public static ColumnHolder getRandomDateTimeCol(String colName, int size, Random random) {
        final DBDateTime[] data = new DBDateTime[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = new DBDateTime(random.nextLong());
        }
        return ColumnHolder.createColumnHolder(colName, false, data);
    }

    public static void validate(final EvalNuggetInterface en[]) {
        validate("", en);
    }

    public static void validate(final String ctxt, final EvalNuggetInterface en[]) {
        if (LiveTableTestCase.printTableUpdates) {
            System.out.println();
            System.out.println("================ NEXT ITERATION ================");
        }
        for (int i = 0; i < en.length; i++) {
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                if (LiveTableTestCase.printTableUpdates) {
                    if (i != 0) {
                        System.out.println("================ NUGGET (" + i + ") ================");
                    }
                    try {
                        en[i].show();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    System.out.println();
                }
                en[i].validate(ctxt + " en_i = " + i);
            }
        }
    }

    static Index getInitialIndex(int size, Random random) {
        final Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();
        long firstKey = 10;
        for (int i = 0; i < size; i++) {
            builder.addKey(firstKey = firstKey + random.nextInt(3));
        }
        return builder.getIndex();
    }

    public static Index selectSubIndexSet(int size, Index sourceIndex, Random random) {
        Assert.assertion(size <= sourceIndex.size(), "size <= sourceIndex.size()", size, "size", sourceIndex, "sourceIndex.size()");

        // generate an array that is the size of our index, then shuffle it, and those are the positions we'll pick
        final Integer[] positions = new Integer[(int)sourceIndex.size()];
        for (int ii = 0; ii < positions.length; ++ii) {
            positions[ii] = ii;
        }
        Collections.shuffle(Arrays.asList(positions), random);

        // now create an index with each of our selected positions
        final IndexBuilder resultBuilder = Index.FACTORY.getRandomBuilder();
        for (int ii = 0; ii < size; ++ii) {
            resultBuilder.addKey(sourceIndex.get(positions[ii]));
        }

        return resultBuilder.getIndex();
    }

    public static Index newIndex(int targetSize, Index sourceIndex, Random random) {
        final long maxKey = (sourceIndex.size() == 0 ? 0 : sourceIndex.lastKey());
        final long emptySlots = maxKey - sourceIndex.size();
        final int slotsToFill = Math.min(Math.min((int)(Math.max(0.0, ((random.nextGaussian() / 0.1) + 0.9)) * emptySlots), targetSize), (int)emptySlots);

        final Index fillIn = selectSubIndexSet(slotsToFill, Index.FACTORY.getIndexByRange(0, maxKey).minus(sourceIndex), random);

        final int endSlots = targetSize - (int)fillIn.size();

        double density = ((random.nextGaussian() / 0.25) + 0.5);
        density = density < 0.1 ? 0.1 : density;
        density = density > 1 ? 1 : density;
        final long rangeSize = (long)((1.0 / density) * endSlots);
        final Index expansion = selectSubIndexSet(endSlots, Index.FACTORY.getIndexByRange(maxKey + 1, maxKey + rangeSize + 1), random);

        fillIn.insert(expansion);

        Assert.assertion(fillIn.size() == targetSize, "fillIn.size() == targetSize", fillIn.size(), "fillIn.size()", targetSize, "targetSize", endSlots, "endSlots", slotsToFill, "slotsToFill");

        return fillIn;
    }

    public static ColumnInfo[] initColumnInfos(String names[], Generator... generators) {
        if (names.length != generators.length) {
            throw new IllegalArgumentException("names and generator lengths mismatch: " + names.length + " != " + generators.length);
        }

        final ColumnInfo[] result = new ColumnInfo[names.length];
        for (int i = 0; i < result.length; i++) {
            //noinspection unchecked
            result[i] = new ColumnInfo(generators[i], names[i]);
        }
        return result;
    }

    public static ColumnInfo[] initColumnInfos(String names[], ColumnInfo.ColAttributes attributes[], Generator... generators) {
        if (names.length != generators.length) {
            throw new IllegalArgumentException("names and generator lengths mismatch: " + names.length + " != " + generators.length);
        }

        final ColumnInfo[] result = new ColumnInfo[names.length];
        for (int i = 0; i < result.length; i++) {
            //noinspection unchecked
            result[i] = new ColumnInfo(generators[i], names[i], attributes);
        }
        return result;
    }

    public static ColumnInfo[] initColumnInfos(String names[], List<List<ColumnInfo.ColAttributes>> attributes, Generator... generators) {
        if (names.length != generators.length) {
            throw new IllegalArgumentException("names and generator lengths mismatch: " + names.length + " != " + generators.length);
        }

        final ColumnInfo[] result = new ColumnInfo[names.length];
        for (int ii = 0; ii < result.length; ii++) {
            //noinspection unchecked
            result[ii] = new ColumnInfo(generators[ii], names[ii], attributes.get(ii).toArray(ColumnInfo.ZERO_LENGTH_COLUMN_ATTRIBUTES_ARRAY));
        }
        return result;
    }

    public static QueryTable getTable(int size, Random random, ColumnInfo columnInfos[]) {
        return getTable(true, size, random, columnInfos);
    }

    public static QueryTable getTable(boolean refreshing, int size, Random random, ColumnInfo columnInfos[]) {
        final Index index = getInitialIndex(size, random);
        for (ColumnInfo columnInfo : columnInfos) {
            columnInfo.populateMap(index, random);
        }
        final ColumnHolder[] sources = new ColumnHolder[columnInfos.length];
        for (int i = 0; i < columnInfos.length; i++) {
            sources[i] = columnInfos[i].c();

        }
        if (refreshing) {
            return testRefreshingTable(index, sources);
        } else {
            return testTable(index, sources);
        }
    }

    public static QueryTable testTable(ColumnHolder... columnHolders) {
        final Object[] boxedData = ArrayUtils.getBoxedArray(columnHolders[0].data);
        final Index index = Index.FACTORY.getFlatIndex(boxedData.length);
        return testTable(index, columnHolders);
    }

    public static QueryTable testTable(Index index, ColumnHolder... columnHolders) {
        final Map<String, ColumnSource> columns = new LinkedHashMap<>();
        for (ColumnHolder columnHolder : columnHolders) {
            columns.put(columnHolder.name, getTreeMapColumnSource(index, columnHolder));
        }
        return new QueryTable(index, columns);
    }

    public static QueryTable testRefreshingTable(Index index, ColumnHolder... columnHolders) {
        final QueryTable queryTable = testTable(index, columnHolders);
        queryTable.setRefreshing(true);
        return queryTable;
    }

    public static QueryTable testFlatRefreshingTable(Index index, ColumnHolder... columnHolders) {
        Assert.assertion(index.isFlat(), "index.isFlat()", index, "index");
        final QueryTable queryTable = testTable(index, columnHolders);
        queryTable.setRefreshing(true);
        queryTable.setFlat();
        return queryTable;
    }

    public static QueryTable testRefreshingTable(ColumnHolder... columnHolders) {
        final Index index = columnHolders.length == 0 ? Index.FACTORY.getEmptyIndex() : Index.FACTORY.getFlatIndex(Array.getLength(columnHolders[0].data));
        final Map<String, ColumnSource> columns = new LinkedHashMap<>();
        for (ColumnHolder columnHolder : columnHolders) {
            columns.put(columnHolder.name, getTreeMapColumnSource(index, columnHolder));
        }
        final QueryTable queryTable = new QueryTable(index, columns);
        queryTable.setRefreshing(true);
        return queryTable;
    }

    public static ColumnSource getTreeMapColumnSource(Index index, ColumnHolder columnHolder) {
        final Object[] boxedData = ArrayUtils.getBoxedArray(columnHolder.data);

        final AbstractColumnSource result;
        if (columnHolder instanceof ImmutableColumnHolder) {
            //noinspection unchecked
            result = new ImmutableTreeMapSource(columnHolder.dataType, index, boxedData);
        } else if (columnHolder.dataType.equals(DBDateTime.class) && columnHolder.data instanceof long[]) {
            result = new DateTimeTreeMapSource(index, (long[])columnHolder.data);
        } else {
            //noinspection unchecked
            result = new TreeMapSource(columnHolder.dataType, index, boxedData);
        }

        if (columnHolder.grouped) {
            //noinspection unchecked
            result.setGroupToRange(result.getValuesMapping(index));
        }
        return result;
    }

    public static Table prevTableColumnSources(Table table) {
        final Index index = table.getIndex().getPrevIndex();
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        table.getColumnSourceMap().forEach((k, cs) -> {
            //noinspection unchecked
            columnSourceMap.put(k, new PrevColumnSource(cs));
        });
        return new QueryTable(index, columnSourceMap);
    }

    public static Table prevTable(Table table) {
        final Index index = table.getIndex().getPrevIndex();

        final List<ColumnHolder<?>> cols = new ArrayList<>();
        for (Map.Entry<String, ? extends ColumnSource> mapEntry : table.getColumnSourceMap().entrySet()) {
            final String name = mapEntry.getKey();
            final ColumnSource<?> columnSource = mapEntry.getValue();
            final List<Object> data = new ArrayList<>();

            for (final Index.Iterator it = index.iterator(); it.hasNext(); ) {
                final long key = it.nextLong();
                final Object item = columnSource.getPrev(key);
                data.add(item);
            }

            if (columnSource.getType() == int.class) {
                cols.add(new ColumnHolder<>(name, false, data.stream().mapToInt(x -> x == null ? io.deephaven.util.QueryConstants.NULL_INT : (int) x).toArray()));
            } else if (columnSource.getType() == long.class) {
                cols.add(new ColumnHolder<>(name, false, data.stream().mapToLong(x -> x == null ? io.deephaven.util.QueryConstants.NULL_LONG : (long)x).toArray()));
            } else if (columnSource.getType() == boolean.class) {
                cols.add(ColumnHolder.createColumnHolder(name, false, data.stream().map(x -> (Boolean)x).toArray(Boolean[]::new)));
            } else if (columnSource.getType() == String.class) {
                cols.add(ColumnHolder.createColumnHolder(name, false, data.stream().map(x -> (String)x).toArray(String[]::new)));
            } else if (columnSource.getType() == double.class) {
                cols.add(new ColumnHolder<>(name, false, data.stream().mapToDouble(x -> x == null ? io.deephaven.util.QueryConstants.NULL_DOUBLE : (double)x).toArray()));
            } else if (columnSource.getType() == float.class) {
                final float [] floatArray = new float[data.size()];
                for (int ii = 0; ii < data.size(); ++ii) {
                    final Object value = data.get(ii);
                    floatArray[ii] = value == null ? QueryConstants.NULL_FLOAT : (float)value;
                }
                cols.add(new ColumnHolder<>(name, false, floatArray));
            } else if (columnSource.getType() == char.class) {
                final char [] charArray = new char[data.size()];
                for (int ii = 0; ii < data.size(); ++ii) {
                    final Object value = data.get(ii);
                    charArray[ii] = value == null ? QueryConstants.NULL_CHAR : (char)value;
                }
                cols.add(new ColumnHolder<>(name, false, charArray));
            } else if (columnSource.getType() == byte.class) {
                final byte [] byteArray = new byte[data.size()];
                for (int ii = 0; ii < data.size(); ++ii) {
                    final Object value = data.get(ii);
                    byteArray[ii] = value == null ? QueryConstants.NULL_BYTE : (byte)value;
                }
                cols.add(new ColumnHolder<>(name, false, byteArray));
            } else if (columnSource.getType() == short.class) {
                final short [] shortArray = new short[data.size()];
                for (int ii = 0; ii < data.size(); ++ii) {
                    final Object value = data.get(ii);
                    shortArray[ii] = value == null ? QueryConstants.NULL_SHORT : (short)value;
                }
                cols.add(new ColumnHolder<>(name, false, shortArray));
            } else {
                cols.add(new ColumnHolder(name, columnSource.getType(), columnSource.getComponentType(), false, data.toArray((Object[])Array.newInstance(columnSource.getType(), data.size()))));
            }
        }

        return TableTools.newTable(cols.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

    public interface Generator<T, U> {
        TreeMap<Long, U> populateMap(TreeMap<Long, U> values, Index toAdd, Random random);

        /**
         * Called after a key has been removed from the map.
         *
         * @param key     the index key that was removed
         * @param removed the value that was removed
         */
        default void onRemove(long key, U removed) {
        }

        default void onMove(long oldKey, long newKey, U moved) {
        }

        Class<U> getType();

        Class<T> getColumnType();
    }

    public static abstract class AbstractReinterpretedGenerator<T, U> implements Generator<T, U> {
        @Override
        public TreeMap<Long, U> populateMap(final TreeMap<Long, U> values, final Index toAdd, final Random random) {
            final TreeMap<Long, U> result = new TreeMap<>();
            toAdd.forAllLongs((final long nextKey) -> {
                final U value = nextValue(values, nextKey, random);
                result.put(nextKey, value);
                values.put(nextKey, value);
            });
            return result;
        }

        abstract U nextValue(TreeMap<Long, U> values, long key, Random random);
    }

    public static abstract class AbstractGenerator<T> extends AbstractReinterpretedGenerator<T, T> {
        public Class<T> getColumnType() {
            return getType();
        }
    }


    public static class SetGenerator<T> extends AbstractGenerator<T> {

        private final T[] set;
        private final Class<T> type;

        public SetGenerator(T... set) {
            this.set = set;
            //noinspection unchecked
            type = (Class<T>)set.getClass().getComponentType();
        }

        public SetGenerator(Class<T> type, Collection<T> set) {
            this.type = type;
            //noinspection unchecked
            this.set = set.toArray((T[])Array.newInstance(type, set.size()));
        }

        @Override
        public T nextValue(TreeMap<Long, T> values, long key, Random random) {
            return set[random.nextInt(set.length)];
        }

        @Override
        public Class<T> getType() {
            return type;
        }
    }

    public static class StringGenerator extends AbstractGenerator<String> {
        int bound;

        public StringGenerator() {
            bound = 0;
        }

        public StringGenerator(int bound) {
            this.bound = bound;
        }

        @Override
        public String nextValue(TreeMap<Long, String> values, long key, Random random) {
            final long value = bound > 0 ? random.nextInt(bound) : random.nextLong();
            return Long.toString(value, 'z' - 'a' + 10);
        }

        @Override
        public Class<String> getType() {
            return String.class;
        }
    }


    public static class IntGenerator extends AbstractGenerator<Integer> {

        private final int to, from;
        private final double nullFraction;

        public IntGenerator() {
            this(-Integer.MAX_VALUE / 2, Integer.MAX_VALUE / 2);
        }

        public IntGenerator(int from, int to) {
            this(from, to, 0);
        }

        public IntGenerator(int from, int to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Integer nextValue(TreeMap<Long, Integer> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }
            return from + random.nextInt(to - from);
        }

        @Override
        public Class<Integer> getType() {
            return Integer.class;
        }
    }

    public static class BooleanGenerator extends AbstractGenerator<Boolean> {

        private final double trueFraction;
        private final double nullFraction;

        public BooleanGenerator() {
            this(0.5, 0);
        }

        public BooleanGenerator(double trueFraction) {
            this(trueFraction, 0);
        }

        public BooleanGenerator(double trueFraction, double nullFraction) {
            this.trueFraction = trueFraction;
            this.nullFraction = nullFraction;
        }

        @Override
        public Boolean nextValue(TreeMap<Long, Boolean> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }
            return random.nextDouble() < trueFraction;
        }

        @Override
        public Class<Boolean> getType() {
            return Boolean.class;
        }
    }

    public static class BigIntegerGenerator extends AbstractGenerator<BigInteger> {
        private static final BigInteger DEFAULT_FROM = BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2));
        private static final BigInteger DEFAULT_TO = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2));

        private final BigInteger to, from;
        private final double nullFraction;
        private final int rangeBitLength;
        private final BigInteger range;

        public BigIntegerGenerator() {
            this(DEFAULT_FROM, DEFAULT_TO);
        }

        public BigIntegerGenerator(double nullFraction) {
            this(BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2)), BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)), 0);
        }

        public BigIntegerGenerator(BigInteger from, BigInteger to) {
            this(from, to, 0);
        }

        public BigIntegerGenerator(BigInteger from, BigInteger to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;

            range = to.subtract(from);
            rangeBitLength = range.bitLength();
        }

        @Override
        public BigInteger nextValue(TreeMap<Long, BigInteger> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }

            BigInteger value;
            do {
                value = new BigInteger(rangeBitLength, random);
            } while (value.compareTo(range) > 0);

            final BigInteger result = value.add(from);

            Assert.assertion(result.compareTo(from) >= 0, "result.compareTo(from) >= 0", result, "result", from, "from");
            Assert.assertion(result.compareTo(to) <= 0, "result.compareTo(to) <= 0", result, "result", to, "to");

            return result;
        }

        @Override
        public Class<BigInteger> getType() {
            return BigInteger.class;
        }
    }


    public static class BigDecimalGenerator extends AbstractGenerator<BigDecimal> {

        static final BigInteger DEFAULT_FROM = BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2));
        static final BigInteger DEFAULT_TO = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2));
        private final BigInteger to, from;
        private final BigDecimal toDecimal, fromDecimal;
        private final double nullFraction;
        private final int rangeBitLength;
        private final int decimalPlaces;
        private final BigInteger range;

        public BigDecimalGenerator() {
            this(DEFAULT_FROM, DEFAULT_TO, 10, 0);
        }

        public BigDecimalGenerator(double nullFraction) {
            this(DEFAULT_FROM, DEFAULT_TO, 10, nullFraction);
        }

        public BigDecimalGenerator(BigInteger from, BigInteger to) {
            this(from, to, 10, 0);
        }

        public BigDecimalGenerator(BigInteger from, BigInteger to, int decimalPlaces, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
            final BigInteger scale = BigInteger.TEN.pow(decimalPlaces);
            this.decimalPlaces = decimalPlaces;

            range = to.subtract(from).multiply(scale);
            rangeBitLength = range.bitLength();

            toDecimal = new BigDecimal(to);
            fromDecimal = new BigDecimal(from);
        }

        @Override
        public BigDecimal nextValue(TreeMap<Long, BigDecimal> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }

            BigInteger value;
            do {
                value = new BigInteger(rangeBitLength, random);
            } while (value.compareTo(range) > 0);

            final BigDecimal result = new BigDecimal(value, decimalPlaces).add(fromDecimal);

            Assert.assertion(result.compareTo(fromDecimal) >= 0, "result.compareTo(from) >= 0", result, "result", from, "from");
            Assert.assertion(result.compareTo(toDecimal) <= 0, "result.compareTo(to) <= 0", result, "result", to, "to");

            return result;
        }

        @Override
        public Class<BigDecimal> getType() {
            return BigDecimal.class;
        }
    }

    public static class ShortGenerator extends AbstractGenerator<Short> {

        private final short to, from;
        private final double nullFraction;

        public ShortGenerator() {
            this((short)(QueryConstants.NULL_SHORT + 1), Short.MAX_VALUE);
        }

        public ShortGenerator(short from, short to) {
            this(from, to, 0);
        }

        public ShortGenerator(short from, short to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Short nextValue(TreeMap<Long, Short> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }
            return (short)(from + random.nextInt(to - from));
        }

        @Override
        public Class<Short> getType() {
            return Short.class;
        }
    }

    public static class ByteGenerator extends AbstractGenerator<Byte> {

        private final byte to, from;
        private final double nullFraction;

        public ByteGenerator() {
            this((byte)(QueryConstants.NULL_BYTE + 1), Byte.MAX_VALUE);
        }

        public ByteGenerator(byte from, byte to) {
            this(from, to, 0);
        }

        public ByteGenerator(byte from, byte to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Byte nextValue(TreeMap<Long, Byte> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }
            return (byte)(from + random.nextInt(to - from));
        }

        @Override
        public Class<Byte> getType() {
            return Byte.class;
        }
    }

    public static class CharGenerator extends AbstractGenerator<Character> {

        private final char to, from;
        private final double nullFraction;

        public CharGenerator(char from, char to) {
            this.from = from;
            this.to = to;
            nullFraction = 0.0;
        }

        public CharGenerator(char from, char to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Character nextValue(TreeMap<Long, Character> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }
            return (char)(from + random.nextInt(to - from));
        }

        @Override
        public Class<Character> getType() {
            return Character.class;
        }
    }

    public static class LongGenerator extends AbstractGenerator<Long> {

        private final long to, from;
        private final double nullFraction;

        public LongGenerator() {
            this(QueryConstants.NULL_LONG + 1, Long.MAX_VALUE);
        }

        public LongGenerator(long from, long to) {
            this(from, to, 0.0);
        }

        public LongGenerator(long from, long to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Long nextValue(TreeMap<Long, Long> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }
            final long distance = to - from;
            if (distance > 0 && distance < Integer.MAX_VALUE) {
                return from + random.nextInt((int)(to - from));
            } else if (from == QueryConstants.NULL_LONG + 1 && to == Long.MAX_VALUE) {
                long r;
                do {
                    r = random.nextLong();
                } while (r == QueryConstants.NULL_LONG);
                return r;
            } else {
                return (long)(from + random.nextDouble() * (to - from));
            }
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }
    }

    public static class DoubleGenerator extends AbstractGenerator<Double> {

        private final double to, from;
        private final double nullFraction;
        private final double nanFraction;
        private final double negInfFraction;
        private final double posInfFraction;

        public DoubleGenerator() {
            this(QueryConstants.NULL_DOUBLE + 1, Double.MAX_VALUE);
        }

        public DoubleGenerator(double from, double to) {
            this(from, to, 0);
        }

        public DoubleGenerator(double from, double to, double nullFraction) {
            this(from, to, nullFraction, 0.0);
        }

        public DoubleGenerator(double from, double to, double nullFraction, double nanFraction) {
            this(from, to, nullFraction, nanFraction, 0, 0);
        }

        public DoubleGenerator(double from, double to, double nullFraction, double nanFraction, double negInfFraction, double posInfFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
            this.nanFraction = nanFraction;
            this.negInfFraction = negInfFraction;
            this.posInfFraction = posInfFraction;
            Require.leq(nullFraction + nanFraction + negInfFraction + posInfFraction, "nullFraction + nanFraction + negInfFraction + posInfFraction", 1.0, "1.0");
        }

        @Override
        public Double nextValue(TreeMap<Long, Double> values, long key, Random random) {
            if (nullFraction > 0 || nanFraction > 0 || negInfFraction > 0 || posInfFraction > 0) {
                final double frac = random.nextDouble();

                if (nullFraction > 0 && frac < nullFraction) {
                    return null;
                }

                if (nanFraction > 0 && frac < (nullFraction + nanFraction)) {
                    return Double.NaN;
                }

                if (negInfFraction > 0 && frac < (nullFraction + nanFraction + negInfFraction)) {
                    return Double.NEGATIVE_INFINITY;
                }

                if (posInfFraction > 0 && frac < (nullFraction + nanFraction + negInfFraction + posInfFraction)) {
                    return Double.POSITIVE_INFINITY;
                }
            }
            return from + (random.nextDouble() * to - from);
        }

        @Override
        public Class<Double> getType() {
            return Double.class;
        }
    }

    public static class FloatGenerator extends AbstractGenerator<Float> {

        private final float to, from;
        private final double nullFraction;
        private final double nanFraction;
        private final double negInfFraction;
        private final double posInfFraction;

        public FloatGenerator() {
            this(QueryConstants.NULL_FLOAT + 1, Float.MAX_VALUE);
        }

        public FloatGenerator(float from, float to) {
            this(from, to, 0);
        }

        public FloatGenerator(float from, float to, double nullFraction) {
            this(from, to, nullFraction, 0.0);
        }

        public FloatGenerator(float from, float to, double nullFraction, double nanFraction) {
            this(from, to, nullFraction, nanFraction, 0, 0);
        }

        public FloatGenerator(float from, float to, double nullFraction, double nanFraction, double negInfFraction, double posInfFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
            this.nanFraction = nanFraction;
            this.negInfFraction = negInfFraction;
            this.posInfFraction = posInfFraction;
            Require.leq(nullFraction + nanFraction + negInfFraction + posInfFraction, "nullFraction + nanFraction + negInfFraction + posInfFraction", 1.0, "1.0");
        }

        @Override
        public Float nextValue(TreeMap<Long, Float> values, long key, Random random) {
            if (nullFraction > 0 || nanFraction > 0 || negInfFraction > 0 || posInfFraction > 0) {
                final double frac = random.nextDouble();

                if (nullFraction > 0 && frac < nullFraction) {
                    return null;
                }

                if (nanFraction > 0 && frac < (nullFraction + nanFraction)) {
                    return Float.NaN;
                }

                if (negInfFraction > 0 && frac < (nullFraction + nanFraction + negInfFraction)) {
                    return Float.NEGATIVE_INFINITY;
                }

                if (posInfFraction > 0 && frac < (nullFraction + nanFraction + negInfFraction + posInfFraction)) {
                    return Float.POSITIVE_INFINITY;
                }
            }
            return from + (random.nextFloat() * to - from);
        }

        @Override
        public Class<Float> getType() {
            return Float.class;
        }
    }

    public static class SortedIntGenerator extends AbstractSortedGenerator<Integer> {
        private final Integer minInteger;
        private final Integer maxInteger;

        public SortedIntGenerator(Integer minInteger, Integer maxInteger) {
            this.minInteger = minInteger;
            this.maxInteger = maxInteger;
        }

        Integer maxValue() {
            return maxInteger;
        }

        Integer minValue() {
            return minInteger;
        }

        Integer makeValue(Integer floor, Integer ceiling, Random random) {
            return floor + random.nextInt(ceiling - floor + 1);
        }

        @Override
        public Class<Integer> getType() {
            return Integer.class;
        }
    }

    public static class IncreasingSortedLongGenerator extends AbstractGenerator<Long> {
        private final int step;
        private long lastValue;

        public IncreasingSortedLongGenerator(int step, long startValue) {
            this.step = step;
            this.lastValue = startValue;
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }

        @Override
        Long nextValue(TreeMap<Long, Long> values, long key, Random random) {
            lastValue += random.nextInt(step);
            return lastValue;
        }
    }

    public static class SortedLongGenerator extends AbstractSortedGenerator<Long> {
        private final long minValue;
        private final long maxValue;

        public SortedLongGenerator(long minValue, long maxValue) {
            if (maxValue == Long.MAX_VALUE) {
                // Because the "range + 1" code below makes it wrap.
                throw new UnsupportedOperationException("Long.MAX_VALUE not supported");
            }
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        Long maxValue() {
            return maxValue;
        }

        Long minValue() {
            return minValue;
        }

        Long makeValue(Long floor, Long ceiling, Random random) {
            final long range = ceiling - floor;
            if (range + 1 < Integer.MAX_VALUE) {
                return floor + random.nextInt((int)(range + 1));
            }
            final int bits = 64 - Long.numberOfLeadingZeros(range);
            long candidate = getRandom(random, bits);
            while (candidate > range || candidate < 0) {
                candidate = getRandom(random, bits);
            }
            return floor + candidate;
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }
    }

    public static class SortedBigIntegerGenerator extends AbstractSortedGenerator<BigInteger> {
        private final BigInteger minValue;
        private final BigInteger maxValue;

        public SortedBigIntegerGenerator(int numBits) {
            minValue = BigInteger.ZERO;
            BigInteger mv = BigInteger.ZERO;
            mv = mv.setBit(numBits + 1).subtract(BigInteger.ONE);
            maxValue = mv;
        }

        @Override
        BigInteger minValue() {
            return minValue;
        }

        @Override
        BigInteger maxValue() {
            return maxValue;
        }

        @Override
        BigInteger makeValue(BigInteger floor, BigInteger ceiling, Random random) {
            final BigInteger range = ceiling.subtract(floor);
            final int allowedBits = range.bitLength();
            BigInteger candidate = null;
            for (int ii = 0; ii < 100; ++ii) {
              candidate = new BigInteger(allowedBits, random);
              if (candidate.compareTo(range) < 0) {
                  break;
              }
              candidate = null;
            }
            if (candidate == null) {
                throw new RuntimeException(String.format("Couldn't find a suitable BigInteger between %s and %s",
                        floor, ceiling));
            }
            return floor.add(candidate);
        }

        @Override
        public Class<BigInteger> getType() {
            return BigInteger.class;
        }
    }

    public static class SortedDoubleGenerator extends AbstractSortedGenerator<Double> {
        private final double minValue;
        private final double maxValue;

        public SortedDoubleGenerator(double minValue, double maxValue) {
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        Double maxValue() {
            return maxValue;
        }

        Double minValue() {
            return minValue;
        }

        Double makeValue(Double floor, Double ceiling, Random random) {
            return floor + (random.nextDouble() * (ceiling - floor));
        }

        @Override
        public Class<Double> getType() {
            return Double.class;
        }
    }

    private static long getRandom(Random random, int bits) {
        final long value = random.nextLong();
        return bits >= 64 ? value : ((1L << bits) - 1L) & value;
    }

    public static class SortedDateTimeGenerator extends AbstractSortedGenerator<DBDateTime> {
        private final DBDateTime minTime;
        private final DBDateTime maxTime;

        public SortedDateTimeGenerator(DBDateTime minTime, DBDateTime maxTime) {
            this.minTime = minTime;
            this.maxTime = maxTime;
        }

        DBDateTime maxValue() {
            return maxTime;
        }

        DBDateTime minValue() {
            return minTime;
        }

        DBDateTime makeValue(DBDateTime floor, DBDateTime ceiling, Random random) {
            final long longFloor = floor.getNanos();
            final long longCeiling = ceiling.getNanos();

            final long range = longCeiling - longFloor + 1L;
            final long nextLong = Math.abs(random.nextLong()) % range;

            return new DBDateTime(longFloor + (nextLong % range));
        }

        @Override
        public Class<DBDateTime> getType() {
            return DBDateTime.class;
        }
    }

    public static class UnsortedDateTimeGenerator extends AbstractGenerator<DBDateTime> {
        private final DBDateTime minTime;
        private final DBDateTime maxTime;
        private final double nullFrac;

        public UnsortedDateTimeGenerator(DBDateTime minTime, DBDateTime maxTime) {
            this(minTime, maxTime, 0);
        }

        public UnsortedDateTimeGenerator(DBDateTime minTime, DBDateTime maxTime, double nullFrac) {
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.nullFrac = nullFrac;
        }

        @Override
        public Class<DBDateTime> getType() {
            return DBDateTime.class;
        }

        @Override
        DBDateTime nextValue(TreeMap<Long, DBDateTime> values, long key, Random random) {
            if (nullFrac > 0 && random.nextDouble() < nullFrac) {
                return null;
            }
            final long longFloor = minTime.getNanos();
            final long longCeiling = maxTime.getNanos();

            final long range = longCeiling - longFloor + 1L;
            final long nextLong = Math.abs(random.nextLong()) % range;

            return new DBDateTime(longFloor + (nextLong % range));
        }
    }

    public static class UnsortedDateTimeLongGenerator extends AbstractReinterpretedGenerator<DBDateTime, Long> {
        private final DBDateTime minTime;
        private final DBDateTime maxTime;
        private final double nullFrac;

        public UnsortedDateTimeLongGenerator(DBDateTime minTime, DBDateTime maxTime) {
            this(minTime, maxTime, 0);
        }

        public UnsortedDateTimeLongGenerator(DBDateTime minTime, DBDateTime maxTime, double nullFrac) {
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.nullFrac = nullFrac;
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }

        @Override
        public Class<DBDateTime> getColumnType() {
            return DBDateTime.class;
        }

        @Override
        Long nextValue(TreeMap<Long, Long> values, long key, Random random) {
            if (nullFrac > 0 && random.nextDouble() < nullFrac) {
                return null;
            }
            final long longFloor = minTime.getNanos();
            final long longCeiling = maxTime.getNanos();

            final long range = longCeiling - longFloor + 1L;
            final long nextLong = Math.abs(random.nextLong()) % range;

            return new DBDateTime(longFloor + (nextLong % range)).getNanos();
        }
    }

    public static class DateGenerator extends AbstractSortedGenerator<Date> {
        private final Date minDate;
        private final Date maxDate;

        public DateGenerator(Date minDate, Date maxDate) {
            this.minDate = minDate;
            this.maxDate = maxDate;
        }


        Date maxValue() {
            return maxDate;
        }

        Date minValue() {
            return minDate;
        }

        Date makeValue(Date floor, Date ceiling, Random random) {
            if (floor.equals(ceiling)) {
                return floor;
            }

            if (ceiling.getTime() < floor.getTime()) {
                throw new IllegalStateException("ceiling < floor: " + ceiling + " > " + floor);
            }
            return new Date(floor.getTime() + random.nextInt((int)(ceiling.getTime() - floor.getTime() + 1)));
        }

        @Override
        public Class<Date> getType() {
            return Date.class;
        }
    }

    static abstract class AbstractSortedGenerator<T extends Comparable<? super T>> implements Generator<T, T> {
        public TreeMap<Long, T> populateMap(TreeMap<Long, T> values, Index toAdd, Random random) {
            final TreeMap<Long, T> result = new TreeMap<>();
            if (toAdd.size() == 0)
                return result;

            for (final Index.Iterator it = toAdd.iterator(); it.hasNext(); ) {
                values.remove(it.nextLong());
            }

            final Index.Iterator iterator = toAdd.iterator();
            long firstKey = iterator.nextLong();
            long lastKey = firstKey;
            final Map.Entry<Long, T> firstFloorEntry = values.floorEntry(firstKey);
            T currentFloor = firstFloorEntry == null ? minValue() : firstFloorEntry.getValue();

            final Map.Entry<Long, T> firstCeilingEntry = values.ceilingEntry(firstKey);
            T currentCeiling = firstCeilingEntry == null ? maxValue() : firstCeilingEntry.getValue();

            while (iterator.hasNext()) {
                final long nextKey = iterator.nextLong();
                final Map.Entry<Long, T> ceilingEntry = values.ceilingEntry(nextKey);
                final Map.Entry<Long, T> floorEntry = values.floorEntry(nextKey);

                final T floor = floorEntry == null ? minValue() : floorEntry.getValue();
                final T ceiling = ceilingEntry == null ? maxValue() : ceilingEntry.getValue();

                if (!ceiling.equals(currentCeiling) || !floor.equals(currentFloor)) {
                    // we're past the end of the last run so we need to generate the values for the map
                    generateValues(toAdd.intersect(Index.FACTORY.getIndexByRange(firstKey, lastKey)), currentFloor, currentCeiling, result, random);
                    firstKey = nextKey;
                    currentFloor = floor;
                    currentCeiling = ceiling;
                }
                lastKey = nextKey;
            }

            generateValues(toAdd.intersect(Index.FACTORY.getIndexByRange(firstKey, lastKey)), currentFloor, currentCeiling, result, random);

            values.putAll(result);

            checkSorted(values);

            return result;
        }

        abstract T maxValue();

        abstract T minValue();

        private void checkSorted(TreeMap<Long, T> values) {
            T lastValue = minValue();
            for (Map.Entry<Long, T> valueEntry : values.entrySet()) {
                final T value = valueEntry.getValue();
                Assert.assertion(value.compareTo(lastValue) >= 0, "value >= lastValue", value, "value", lastValue, "lastValue", valueEntry.getKey(), "valueEntry.getKey");
                lastValue = value;
            }
        }

        private void generateValues(Index toadd, T floor, T ceiling, TreeMap<Long, T> result, Random random) {
            final int count = (int)toadd.size();
            //noinspection unchecked
            final T[] values = (T[])Array.newInstance(getType(), count);
            for (int ii = 0; ii < count; ++ii) {
                values[ii] = makeValue(floor, ceiling, random);
            }
            Arrays.sort(values);
            int ii = 0;
            for (final Index.Iterator it = toadd.iterator(); it.hasNext(); ) {
                result.put(it.nextLong(), values[ii++]);
            }
        }

        abstract T makeValue(T floor, T ceiling, Random random);

        public Class<T> getColumnType() {
            return getType();
        }
    }

    static abstract class AbstractUniqueGenerator<T> implements Generator<T, T> {
        final Set<T> generatedValues = new HashSet<>();

        public TreeMap<Long, T> populateMap(TreeMap<Long, T> values, Index toAdd, Random random) {
            final TreeMap<Long, T> result = new TreeMap<>();
            if (toAdd.size() == 0)
                return result;

            for (final Index.Iterator it = toAdd.iterator(); it.hasNext(); ) {
                values.remove(it.nextLong());
            }

            final HashSet<T> usedValues = new HashSet<>(values.values());

            for (final Index.Iterator iterator = toAdd.iterator(); iterator.hasNext(); ) {
                final long nextKey = iterator.nextLong();
                final T value = getNextUniqueValue(usedValues, values, nextKey, random);
                usedValues.add(value);
                result.put(nextKey, value);
                values.put(nextKey, value);
            }

            return result;
        }

        // TODO: update the callers so that as we remove rows, we also remove them from the usedValues set;
        // otherwise we can exhaust the set more easily than we should during an incremental update.
        T getNextUniqueValue(Set<T> usedValues, TreeMap<Long, T> values, long key, Random random) {
            T candidate;
            int triesLeft = 20;

            do {
                if (triesLeft-- <= 0) {
                    throw new RuntimeException("Could not generate unique value!");
                }

                candidate = nextValue(values, key, random);
            } while (usedValues.contains(candidate));

            generatedValues.add(candidate);

            return candidate;
        }

        Set<T> getGeneratedValues() {
            return Collections.unmodifiableSet(generatedValues);
        }

        abstract T nextValue(TreeMap<Long, T> values, long key, Random random);

        public Class<T> getColumnType() {
            return getType();
        }
    }

    static class UniqueIntGenerator extends AbstractUniqueGenerator<Integer> {

        private final int to, from;
        private final double nullFraction;

        UniqueIntGenerator(int from, int to) {
            this(from, to, 0.0);
        }

        public UniqueIntGenerator(int from, int to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Integer nextValue(TreeMap<Long, Integer> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }

            return from + random.nextInt(to - from);
        }

        @Override
        public Class<Integer> getType() {
            return Integer.class;
        }
    }

    static class UniqueLongGenerator extends AbstractUniqueGenerator<Long> {

        private final int to, from;
        private final double nullFraction;

        UniqueLongGenerator(int from, int to) {
            this(from, to, 0.0);
        }

        public UniqueLongGenerator(int from, int to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Long nextValue(TreeMap<Long, Long> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }

            return (long)(from + random.nextInt(to - from));
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }
    }

    static class UniqueShortGenerator extends AbstractUniqueGenerator<Short> {

        private final short to, from;
        private final double nullFraction;

        UniqueShortGenerator(short from, short to) {
            this(from, to, 0.0);
        }

        public UniqueShortGenerator(short from, short to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Short nextValue(TreeMap<Long, Short> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }

            return (short)(from + random.nextInt(to - from));
        }

        @Override
        public Class<Short> getType() {
            return Short.class;
        }
    }

    static class UniqueByteGenerator extends AbstractUniqueGenerator<Byte> {

        private final byte to, from;
        private final double nullFraction;

        UniqueByteGenerator(byte from, byte to) {
            this(from, to, 0.0);
        }

        public UniqueByteGenerator(byte from, byte to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Byte nextValue(TreeMap<Long, Byte> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }

            return (byte)(from + random.nextInt(to - from));
        }

        @Override
        public Class<Byte> getType() {
            return Byte.class;
        }
    }

    static class UniqueCharGenerator extends AbstractUniqueGenerator<Character> {
        private final char to, from;
        private final double nullFraction;

        UniqueCharGenerator(char from, char to) {
            this(from, to, 0.0);
        }

        public UniqueCharGenerator(char from, char to, double nullFraction) {
            this.from = from;
            this.to = to;
            this.nullFraction = nullFraction;
        }

        @Override
        public Character nextValue(TreeMap<Long, Character> values, long key, Random random) {
            if (nullFraction > 0) {
                if (random.nextDouble() < nullFraction) {
                    return null;
                }
            }

            return (char)(from + random.nextInt(to - from));
        }

        @Override
        public Class<Character> getType() {
            return Character.class;
        }
    }

    static class UniqueStringGenerator extends AbstractUniqueGenerator<String> {
        @Override
        public String nextValue(TreeMap<Long, String> values, long key, Random random) {
            return Long.toString(random.nextLong(), 'z' - 'a' + 10);
        }

        @Override
        public Class<String> getType() {
            return String.class;
        }
    }

    static class UniqueSmartKeyGenerator extends AbstractUniqueGenerator<SmartKey> {
        private final AbstractGenerator[] generators;

        UniqueSmartKeyGenerator(AbstractGenerator... generators) {
            this.generators = generators;
        }

        @Override
        public SmartKey nextValue(TreeMap<Long, SmartKey> values, long key, Random random) {
            //noinspection unchecked
            return new SmartKey(Arrays.stream(generators).map(g -> g.nextValue(null, key, random)).toArray());
        }

        @Override
        public Class<SmartKey> getType() {
            return SmartKey.class;
        }
    }

    static class SmartKeyGenerator extends AbstractGenerator<SmartKey> {
        private final AbstractGenerator[] generators;

        SmartKeyGenerator(AbstractGenerator... generators) {
            this.generators = generators;
        }

        @Override
        public SmartKey nextValue(TreeMap<Long, SmartKey> values, long key, Random random) {
            //noinspection unchecked
            return new SmartKey(Arrays.stream(generators).map(g -> g.nextValue(null, key, random)).toArray());
        }

        @Override
        public Class<SmartKey> getType() {
            return SmartKey.class;
        }
    }

    static class FromUniqueStringGenerator extends AbstractFromUniqueGenerator<String> {
        FromUniqueStringGenerator(UniqueStringGenerator uniqueStringGenerator, double existingFraction) {
            this(uniqueStringGenerator, existingFraction, new StringGenerator());
        }

        FromUniqueStringGenerator(UniqueStringGenerator uniqueStringGenerator, double existingFraction, AbstractGenerator<String> defaultGenerator) {
            super(String.class, uniqueStringGenerator, defaultGenerator, String[]::new, existingFraction);
        }
    }

    static class FromUniqueSmartKeyGenerator extends AbstractFromUniqueGenerator<SmartKey> {
        FromUniqueSmartKeyGenerator(UniqueSmartKeyGenerator uniqueSmartKeyGenerator, SmartKeyGenerator defaultGenerator, double existingFraction) {
            super(SmartKey.class, uniqueSmartKeyGenerator, defaultGenerator, SmartKey[]::new, existingFraction);
        }
    }

    static class FromUniqueIntGenerator extends AbstractFromUniqueGenerator<Integer> {
        FromUniqueIntGenerator(UniqueIntGenerator uniqueSmartKeyGenerator, IntGenerator defaultGenerator, double existingFraction) {
            super(Integer.class, uniqueSmartKeyGenerator, defaultGenerator, Integer[]::new, existingFraction);
        }
    }

    static class CompositeGenerator<T> implements Generator<T, T> {
        @NotNull
        private final List<Generator<T, T>> generators;
        @NotNull
        private final double[] fractions;

        CompositeGenerator(List<Generator<T, T>> generators, double... fractions) {
            if (fractions.length != generators.size() - 1) {
                throw new IllegalArgumentException("Generators must have one more element than fractions!");
            }
            final double sum = Arrays.stream(fractions).sum();
            if (sum > 1.0) {
                throw new IllegalArgumentException();
            }
            final Generator<T, T> firstGenerator = generators.get(0);
            for (Generator<T, T> generator : generators) {
                if (!generator.getType().equals(firstGenerator.getType())) {
                    throw new IllegalArgumentException("Mismatched generator types: " + generator.getType() + " vs. " + firstGenerator.getType());
                }
                if (!generator.getColumnType().equals(firstGenerator.getColumnType())) {
                    throw new IllegalArgumentException("Mismatched generator column types: " + generator.getType() + " vs. " + firstGenerator.getType());
                }
            }
            this.generators = generators;
            this.fractions = fractions;
        }

        @Override
        public TreeMap<Long, T> populateMap(TreeMap<Long, T> values, Index toAdd, Random random) {
            final TreeMap<Long, T> result = new TreeMap<>();
            if (toAdd.empty()) {
                return result;
            }

            final Index.SequentialBuilder[] builders = new Index.SequentialBuilder[generators.size()];
            for (int ii = 0; ii < builders.length; ++ii) {
                builders[ii] = Index.FACTORY.getSequentialBuilder();
            }
            toAdd.forAllLongs((long ll) -> builders[pickGenerator(random)].appendKey(ll));

            for (int ii = 0; ii < builders.length; ++ii) {
                final Index toAddWithGenerator = builders[ii].getIndex();
                result.putAll(generators.get(ii).populateMap(values, toAddWithGenerator, random));
            }

            return result;
        }

        @Override
        public Class<T> getType() {
            return generators.get(0).getType();
        }

        @Override
        public Class<T> getColumnType() {
            return generators.get(0).getColumnType();
        }

        private int pickGenerator(Random random) {
            final double whichGenerator = random.nextDouble();
            double sum = 0;
            int pickGenerator = 0;
            while (true) {
                if (pickGenerator == generators.size() - 1) {
                    break;
                }
                sum += fractions[pickGenerator];
                if (sum < whichGenerator) {
                    pickGenerator++;
                } else {
                    break;
                }
            }
            return pickGenerator;
        }
    }

    static class AbstractFromUniqueGenerator<T> extends AbstractGenerator<T> {
        private final Class<T> type;
        private final AbstractUniqueGenerator<T> uniqueGenerator;
        private final AbstractGenerator<T> defaultGenerator;
        private final IntFunction<T[]> arrayFactory;
        private final double existingFraction;
        int lastSize = 0;
        T[] lastValues;

        AbstractFromUniqueGenerator(Class<T> type, AbstractUniqueGenerator<T> uniqueStringGenerator, AbstractGenerator<T> defaultGenerator, IntFunction<T[]> arrayFactory, double existingFraction) {
            this.type = type;
            this.uniqueGenerator = uniqueStringGenerator;
            this.defaultGenerator = defaultGenerator;
            this.arrayFactory = arrayFactory;
            this.existingFraction = existingFraction;
        }

        @Override
        public T nextValue(TreeMap<Long, T> values, long key, Random random) {
            if (random.nextDouble() < existingFraction) {
                final int size = uniqueGenerator.getGeneratedValues().size();
                if (size != lastSize) {
                    lastValues = uniqueGenerator.getGeneratedValues().stream().toArray(arrayFactory);
                    lastSize = lastValues.length;
                }
                if (size > 0) {
                    return lastValues[random.nextInt(lastValues.length)];
                }
            }
            return defaultGenerator.nextValue(values, key, random);
        }

        @Override
        public Class<T> getType() {
            return type;
        }
    }

    public static class ColumnInfo<T, U> {
        final Class<T> type;
        final Class<U> dataType;
        final Generator<T, U> generator;
        final String name;
        final TreeMap<Long, U> data;
        final boolean immutable;
        final boolean grouped;

        final static ColAttributes[] ZERO_LENGTH_COLUMN_ATTRIBUTES_ARRAY = new ColAttributes[0];

        enum ColAttributes {
            None,
            Immutable,
            Grouped
        }

        public ColumnInfo(Generator<T, U> generator, String name, ColAttributes... colAttributes) {
            this(generator.getType(), generator.getColumnType(), generator, name, Arrays.asList(colAttributes).contains(ColAttributes.Immutable), Arrays.asList(colAttributes).contains(ColAttributes.Grouped), new TreeMap<>());
        }

        private ColumnInfo(Class<U> dataType, Class<T> type, Generator<T, U> generator, String name, boolean immutable, boolean grouped, TreeMap<Long, U> data) {
            this.dataType = dataType;
            this.type = type;
            this.generator = generator;
            this.name = name;
            this.data = data;
            this.immutable = immutable;
            this.grouped = grouped;
        }

        TreeMap<Long, U> populateMap(Index index, Random random) {
            return generator.populateMap(data, index, random);
        }

        @SuppressWarnings("unchecked")
        public ColumnHolder c() {
            if (dataType == Long.class && type == DBDateTime.class) {
                Require.eqFalse(immutable, "immutable");
                Require.eqFalse(grouped, "grouped");
                final long[] dataArray = data.values().stream().map(x -> (Long)x).mapToLong(x -> x).toArray();
                return ColumnHolder.getDateTimeColumnHolder(name, false, dataArray);
            }

            final U[] dataArray = data.values().toArray((U[])Array.newInstance(dataType, data.size()));
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

        ColumnHolder populateMapAndC(Index keysToModify, Random random) {
            final Collection<U> newValues = populateMap(keysToModify, random).values();
            //noinspection unchecked
            final U[] valueArray = newValues.toArray((U[])Array.newInstance(dataType, newValues.size()));
            if (grouped) {
                return TstUtils.cG(name, valueArray);
            } else {
                return TstUtils.c(name, valueArray);
            }
        }
    }

    public static class StepClock implements Clock, LiveTable {

        private final long nanoTimes[];

        private int step;

        public StepClock(final long... nanoTimes) {
            this.nanoTimes = nanoTimes;
            reset();
        }

        @Override
        public long currentTimeMillis() {
            return nanoTimes[step] / 1_000_000L;
        }

        @Override
        public long currentTimeMicros() {
            return nanoTimes[step] / 1_000L;
        }

        @Override
        public void refresh() {
            step = Math.min(step + 1, nanoTimes.length - 1);
        }

        public void reset() {
            step = 0;
        }
    }


    public static class TstNotification extends AbstractIndexUpdateNotification {

        private boolean invoked = false;

        protected TstNotification() {
            super(false);
        }

        @Override
        public boolean canExecute(final long step) {
            return true;
        }

        @Override
        public void run() {
            assertNotInvoked();
            invoked = true;
        }

        public void reset() {
            invoked = false;
        }

        public void assertInvoked() {
            TestCase.assertTrue(invoked);
        }

        public void assertNotInvoked() {
            TestCase.assertFalse(invoked);
        }
    }

    public static void assertIndexEquals(@NotNull final Index expected, @NotNull final Index actual) {
        try {
            TestCase.assertEquals(expected, actual);
        } catch (AssertionFailedError error) {
            System.err.println("Index equality check failed:"
                    + "\n\texpected: " + expected.toString()
                    + "\n]tactual: " + actual.toString()
                    + "\n]terror: " + error);
            throw error;
        }
    }

    public static void assertTableEquals(@NotNull final Table expected, @NotNull final Table actual, final TableDiff.DiffItems... itemsToSkip) {
        assertTableEquals("", expected, actual, itemsToSkip);
    }

    public static void assertTableEquals(final String context, @NotNull final Table expected, @NotNull final Table actual, final TableDiff.DiffItems... itemsToSkip) {
        if (itemsToSkip.length > 0) {
            assertTableEquals(context, expected, actual, EnumSet.of(itemsToSkip[0], itemsToSkip));
        } else {
            assertTableEquals(context, expected, actual, EnumSet.noneOf(TableDiff.DiffItems.class));
        }
    }

    public static void assertTableEquals(final String context, @NotNull final Table expected, @NotNull final Table actual, final EnumSet<TableDiff.DiffItems> itemsToSkip) {
        final Pair<String, Long> diffPair = TableTools.diffPair(actual, expected, 10, itemsToSkip);
        if (diffPair.getFirst().equals("")) {
            return;
        }
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final long firstRow = Math.max(0, diffPair.getSecond() - 5);
            final long lastRow = Math.max(10, diffPair.getSecond() + 5);

            try (final PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
                TableTools.showWithIndex(expected, firstRow, lastRow, ps);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            final String expectedString = baos.toString();
            baos.reset();

            try (final PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
                TableTools.showWithIndex(actual, firstRow, lastRow, ps);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            final String actualString = baos.toString();

            throw new ComparisonFailure(context + "\n" + diffPair.getFirst(), expectedString, actualString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reruns test cases trying new seeds while minimizing the number of steps to catch the failing test. The test
     * should mutate the provided MutableInt so that when it fails it is equal to the current step iteration. This
     * allows the test to minimize the total number of steps per seed as it discovers better candidate parameters.
     *
     * @param test A test instance to tearDown and setUp between each test.
     * @param initialSeed The seed to begin using.
     * @param maxSeed The highest seed to try.
     * @param initialSteps Number of steps to start with.
     * @param runner A method whose first param is the random seed to use, and second parameter is the number of steps.
     */
    public static void findMinimalTestCase(final LiveTableTestCase test, final int initialSeed, final int maxSeed,
                                           final int initialSteps, final BiConsumer<Integer, MutableInt> runner) {
        final boolean origPrintTableUpdates = LiveTableTestCase.printTableUpdates;
        LiveTableTestCase.printTableUpdates = false;

        int bestSeed = initialSeed;
        int bestSteps = initialSteps;
        boolean failed = false;
        MutableInt maxSteps = new MutableInt(initialSteps);
        for (int seed = initialSeed; seed < maxSeed; ++seed) {
            if (maxSteps.intValue() <= 0) {
                System.out.println("Best Run: bestSeed=" + bestSeed + " bestSteps=" + bestSteps);
                return;
            }
            System.out.println("Running: seed=" + seed + " numSteps=" + maxSteps.intValue() + " bestSeed=" + bestSeed + " bestSteps=" + bestSteps);
            if (seed != initialSeed) {
                try {
                    test.tearDown();
                } catch (Exception | Error e) {
                    System.out.println("Error on test.tearDown():");
                    e.printStackTrace();
                }
                try {
                    test.setUp();
                } catch (Exception | Error e) {
                    System.out.println("Error on test.setUp():");
                    e.printStackTrace();
                }
            }
            try {
                runner.accept(seed, maxSteps);
            } catch (Exception | Error e) {
                failed = true;
                bestSeed = seed;
                bestSteps = maxSteps.intValue() + 1;
                e.printStackTrace();
                System.out.println("Candidate: seed=" + seed + " numSteps=" + (maxSteps.intValue() + 1));
            }
        }

        LiveTableTestCase.printTableUpdates = origPrintTableUpdates;
        if (failed) {
            throw new RuntimeException("Debug candidate: seed=" + bestSeed + " steps=" + bestSteps);
        }
        LiveTableTestCase.printTableUpdates = origPrintTableUpdates;
    }
}
