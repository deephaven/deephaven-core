//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.TableOperationsDefaults;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ColumnSource;
import gnu.trove.map.TByteIntMap;
import gnu.trove.map.TCharIntMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TShortIntMap;
import gnu.trove.map.hash.TByteIntHashMap;
import gnu.trove.map.hash.TCharIntHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TShortIntHashMap;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.tuple.ArrayTuple;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.util.QueryConstants.NULL_INT;

@Category(OutOfBandTest.class)
public class QueryTableStaticNaturalJoinRandomTest extends QueryTableTestBase {
    private final static boolean DO_STATIC_JOIN_PRINT = false;

    private static void testNaturalJoinRandomStatic(int seed, int leftSize, int rightSize, Class<?> dataType,
            boolean grouped, boolean flattenLeft, @Nullable JoinControl control) {
        final Random random = new Random(seed);

        final TestDataGenerator<?, ?> leftGenerator;
        final TestDataGenerator<?, ?> rightGenerator;

        if (dataType == int.class) {
            leftGenerator = new IntGenerator(1, 10 * rightSize);
            rightGenerator = new UniqueIntGenerator(1, rightSize * 8);
        } else if (dataType == short.class) {
            leftGenerator = new ShortGenerator((short) 1, (short) (2 * rightSize));
            rightGenerator = new UniqueShortGenerator((short) 1, (short) (2 * rightSize));
        } else if (dataType == byte.class) {
            leftGenerator = new ByteGenerator((byte) 1, (byte) Math.min(2 * rightSize, Byte.MAX_VALUE));
            rightGenerator = new UniqueByteGenerator((byte) 1, (byte) Math.min(2 * rightSize, Byte.MAX_VALUE));
        } else if (dataType == char.class) {
            leftGenerator = new CharGenerator((char) 1, (char) (2 * rightSize));
            rightGenerator = new UniqueCharGenerator((char) 1, (char) (2 * rightSize));
        } else if (dataType == String.class) {
            final UniqueStringGenerator uniqueStringGenerator = new UniqueStringGenerator();

            final Set<String> duplicateRights = new HashSet<>();
            while (duplicateRights.size() < ((rightSize * 0.1) / 10)) {
                duplicateRights.add("Dup-" + Long.toHexString(random.nextLong()));
            }

            final List<TestDataGenerator<String, String>> generatorList = Arrays.asList(uniqueStringGenerator,
                    new SetGenerator<>(duplicateRights.toArray(String[]::new)));
            rightGenerator = new CompositeGenerator<>(generatorList, 0.9);
            leftGenerator = new FromUniqueStringGenerator(uniqueStringGenerator, 0.5);
        } else if (dataType == ArrayTuple.class) {
            final UniqueArrayTupleGenerator uniqueTupleGenerator =
                    new UniqueArrayTupleGenerator(new LongGenerator(0, 2 * (long) Math.sqrt(rightSize)),
                            new IntGenerator(0, 2 * (int) Math.sqrt(rightSize)));
            final ArrayTupleGenerator defaultGenerator =
                    new ArrayTupleGenerator(new LongGenerator(0, (long) Math.sqrt(rightSize)),
                            new IntGenerator(0, (int) Math.sqrt(rightSize)));
            rightGenerator = uniqueTupleGenerator;
            leftGenerator = new FromUniqueArrayTupleGenerator(uniqueTupleGenerator, defaultGenerator, 0.75);
        } else {
            throw new UnsupportedOperationException("Invalid Data Type: " + dataType);
        }

        final QueryTable rightTable = getTable(false, rightSize, random,
                initColumnInfos(new String[] {"JoinKey", "RightSentinel"},
                        rightGenerator,
                        new IntGenerator()));
        final List<ColumnInfo.ColAttributes> leftKeyAttributes =
                grouped ? Collections.singletonList(ColumnInfo.ColAttributes.Indexed)
                        : Collections.emptyList();
        final QueryTable leftTable = getTable(false, leftSize, random,
                initColumnInfos(new String[] {"JoinKey", "LeftSentinel"},
                        Arrays.asList(leftKeyAttributes, Collections.emptyList()), leftGenerator,
                        new IntGenerator()));

        String matchKeys = "JoinKey";
        Table rightJoinTable = rightTable;
        Table leftJoinTable = leftTable;
        java.util.function.Function<Table, Table> updateFixup = x -> x;

        final String updateString;
        if (dataType == int.class) {
            final TIntIntMap rightMap = new TIntIntHashMap(rightTable.intSize(), 0.5f, -1, NULL_INT);

            final ColumnSource<Integer> rightKey = rightTable.getColumnSource("JoinKey");
            final ColumnSource<Integer> rightSentinel = rightTable.getColumnSource("RightSentinel");
            for (final RowSet.Iterator it = rightTable.getRowSet().iterator(); it.hasNext();) {
                final long next = it.nextLong();
                rightMap.put(rightKey.getInt(next), rightSentinel.getInt(next));
            }
            QueryScope.addParam("rightMap", rightMap);
            updateString = "RightSentinel=rightMap.get(JoinKey)";
        } else if (dataType == short.class) {
            final TShortIntMap rightMap = new TShortIntHashMap(rightTable.intSize(), 0.5f, (short) -1, NULL_INT);

            final ColumnSource<Short> rightKey = rightTable.getColumnSource("JoinKey");
            final ColumnSource<Integer> rightSentinel = rightTable.getColumnSource("RightSentinel");
            for (final RowSet.Iterator it = rightTable.getRowSet().iterator(); it.hasNext();) {
                final long next = it.nextLong();
                rightMap.put(rightKey.getShort(next), rightSentinel.getInt(next));
            }
            QueryScope.addParam("rightMap", rightMap);
            updateString = "RightSentinel=rightMap.get(JoinKey)";
        } else if (dataType == byte.class) {
            final TByteIntMap rightMap = new TByteIntHashMap(rightTable.intSize(), 0.5f, (byte) -1, NULL_INT);

            final ColumnSource<Byte> rightKey = rightTable.getColumnSource("JoinKey");
            final ColumnSource<Integer> rightSentinel = rightTable.getColumnSource("RightSentinel");
            for (final RowSet.Iterator it = rightTable.getRowSet().iterator(); it.hasNext();) {
                final long next = it.nextLong();
                rightMap.put(rightKey.getByte(next), rightSentinel.getInt(next));
            }
            QueryScope.addParam("rightMap", rightMap);
            updateString = "RightSentinel=rightMap.get(JoinKey)";
        } else if (dataType == char.class) {
            final TCharIntMap rightMap = new TCharIntHashMap(rightTable.intSize(), 0.5f, (char) -1, NULL_INT);

            final ColumnSource<Character> rightKey = rightTable.getColumnSource("JoinKey");
            final ColumnSource<Integer> rightSentinel = rightTable.getColumnSource("RightSentinel");
            for (final RowSet.Iterator it = rightTable.getRowSet().iterator(); it.hasNext();) {
                final long next = it.nextLong();
                rightMap.put(rightKey.getChar(next), rightSentinel.getInt(next));
            }
            QueryScope.addParam("rightMap", rightMap);
            updateString = "RightSentinel=rightMap.get(JoinKey)";
        } else if (dataType == String.class) {
            final Map<String, Integer> rightMap = new HashMap<>();

            final ColumnSource<String> rightKey = rightTable.getColumnSource("JoinKey");
            final ColumnSource<Integer> rightSentinel = rightTable.getColumnSource("RightSentinel");
            for (final RowSet.Iterator it = rightTable.getRowSet().iterator(); it.hasNext();) {
                final long next = it.nextLong();
                rightMap.put(rightKey.get(next), rightSentinel.get(next));
            }
            QueryScope.addParam("rightMap", rightMap);
            updateString = "RightSentinel=(int)(rightMap.getOrDefault(JoinKey, null))";
        } else // noinspection ConstantConditions
        if (dataType == ArrayTuple.class) {
            final Map<ArrayTuple, Integer> rightMap = new HashMap<>();

            final ColumnSource<ArrayTuple> rightKey = rightTable.getColumnSource("JoinKey");
            final ColumnSource<Integer> rightSentinel = rightTable.getColumnSource("RightSentinel");
            for (final RowSet.Iterator it = rightTable.getRowSet().iterator(); it.hasNext();) {
                final long next = it.nextLong();
                rightMap.put(rightKey.get(next), rightSentinel.get(next));
            }
            QueryScope.addParam("rightMap", rightMap);
            updateString = "RightSentinel=(int)(rightMap.getOrDefault(JoinKey, null))";

            leftJoinTable = leftTable.update("JoinLong=JoinKey.getElement(0)", "JoinInt=JoinKey.getElement(1)")
                    .dropColumns("JoinKey");
            rightJoinTable = rightTable.update("JoinLong=JoinKey.getElement(0)", "JoinInt=JoinKey.getElement(1)")
                    .dropColumns("JoinKey");
            matchKeys = "JoinLong,JoinInt";
            updateFixup = x -> x.update("JoinLong=JoinKey.getElement(0)", "JoinInt=JoinKey.getElement(1)")
                    .view("LeftSentinel", "JoinLong", "JoinInt", "RightSentinel");
        } else {
            throw new UnsupportedOperationException();
        }

        if (flattenLeft) {
            leftJoinTable = leftJoinTable.flatten();
        }

        if (DO_STATIC_JOIN_PRINT) {
            System.out.println("Left Table:");
            TableTools.showWithRowSet(leftJoinTable, 0, 10);
            System.out.println("Right Table:");
            TableTools.showWithRowSet(rightJoinTable, 10);
        }
        final Table joined;

        if (control == null) {
            joined = leftJoinTable.naturalJoin(rightJoinTable, matchKeys, "RightSentinel");
        } else {
            joined = NaturalJoinHelper.naturalJoin((QueryTable) leftJoinTable, (QueryTable) rightJoinTable,
                    MatchPairFactory.getExpressions(TableOperationsDefaults.splitToCollection(matchKeys)),
                    MatchPairFactory.getExpressions("RightSentinel"), false, control);
        }

        final Table updated = updateFixup.apply(leftTable.update(updateString));

        if (DO_STATIC_JOIN_PRINT) {
            System.out.println("Updated");
            TableTools.showWithRowSet(updated, 0, 10);
            System.out.println("Joined");
            TableTools.showWithRowSet(joined, 0, 10);
        }


        // now make sure it works
        assertTableEquals(updated, joined);
        QueryScope.addParam("rightMap", null);
    }

    public void testNaturalJoinRandomStatic() {
        for (int leftSize = 10; leftSize <= 100_000; leftSize *= 10) {
            for (int rightSize = 10; rightSize <= 100_000; rightSize *= 10) {
                for (int seed = 0; seed < 2; ++seed) {
                    for (Class<?> dataType : Arrays.asList(String.class, int.class, ArrayTuple.class)) {
                        for (boolean grouped : Arrays.asList(Boolean.TRUE, Boolean.FALSE)) {
                            System.out.println("Seed = " + seed + ", leftSize=" + leftSize + ", rightSize=" + rightSize
                                    + ", type=" + dataType + ", grouped=" + grouped);
                            testNaturalJoinRandomStatic(seed, leftSize, rightSize, dataType, grouped, false, null);
                        }
                    }
                }
            }
        }
    }

    public void testNaturalJoinRandomSmallTypes() {
        for (int leftSize = 10; leftSize <= 100_000; leftSize *= 10) {
            final int rightSize = 100;
            for (int seed = 0; seed < 2; ++seed) {
                for (Class<?> dataType : Arrays.asList(byte.class, char.class, short.class)) {
                    for (boolean grouped : Arrays.asList(Boolean.TRUE, Boolean.FALSE)) {
                        System.out.println("Seed = " + seed + ", leftSize=" + leftSize + ", rightSize=" + rightSize
                                + ", type=" + dataType + ", grouped=" + grouped);
                        testNaturalJoinRandomStatic(seed, leftSize, rightSize, dataType, grouped, false, null);
                    }
                }
            }
        }
    }

    // let's force some collisions by making our table small
    public void testNaturalJoinRandomStaticRedirectionBuild() {
        for (int leftSize = 10_000; leftSize <= 10_000; leftSize *= 10) {
            for (int rightSize = 10_000; rightSize <= 10_000; rightSize *= 10) {
                for (int seed = 0; seed < 2; ++seed) {
                    for (Class<?> dataType : Collections.singletonList(int.class)) {
                        for (boolean grouped : Arrays.asList(Boolean.TRUE, Boolean.FALSE)) {
                            for (JoinControl.RedirectionType redirectionType : JoinControl.RedirectionType.values()) {
                                System.out.println("Seed = " + seed + ", leftSize=" + leftSize + ", rightSize="
                                        + rightSize + ", type=" + dataType + ", grouped=" + grouped
                                        + ", redirectionType=" + redirectionType);
                                testNaturalJoinRandomStatic(seed, leftSize, rightSize, dataType, grouped,
                                        redirectionType == JoinControl.RedirectionType.Contiguous, new JoinControl() {
                                            @Override
                                            RedirectionType getRedirectionType(Table leftTable) {
                                                return redirectionType;
                                            }
                                        });
                            }
                        }
                    }
                }
            }
        }
    }
}
