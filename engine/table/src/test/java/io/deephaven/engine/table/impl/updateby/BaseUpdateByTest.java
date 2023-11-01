package io.deephaven.engine.table.impl.updateby;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.generator.*;

import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

public class BaseUpdateByTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    static class CreateResult {
        final QueryTable t;
        final ColumnInfo[] infos;
        final Random random;

        CreateResult(QueryTable t, ColumnInfo[] infos, Random random) {
            this.t = t;
            this.infos = infos;
            this.random = random;
        }
    }

    static CreateResult createTestTable(int tableSize, boolean includeSym, boolean includeGroups, boolean isRefreshing,
            int seed) {
        return createTestTable(tableSize, includeSym, includeGroups, isRefreshing, seed,
                CollectionUtil.ZERO_LENGTH_STRING_ARRAY, new TestDataGenerator[0]);
    }

    @SuppressWarnings({"rawtypes"})
    static CreateResult createTestTable(int tableSize,
            boolean includeSym,
            boolean includeGroups,
            boolean isRefreshing,
            int seed,
            String[] extraNames,
            TestDataGenerator[] extraGenerators) {
        if (includeGroups && !includeSym) {
            throw new IllegalArgumentException();
        }

        final List<String> colsList = new ArrayList<>();
        final List<TestDataGenerator> generators = new ArrayList<>();
        if (includeSym) {
            colsList.add("Sym");
            generators.add(new SetGenerator<>("a", "b", "c", "d", null));
        }

        if (extraNames.length > 0) {
            colsList.addAll(Arrays.asList(extraNames));
            generators.addAll(Arrays.asList(extraGenerators));
        }

        colsList.addAll(Arrays.asList("byteCol", "shortCol", "intCol", "longCol", "floatCol", "doubleCol", "boolCol",
                "bigIntCol", "bigDecimalCol"));
        generators.addAll(Arrays.asList(new ByteGenerator((byte) -127, (byte) 127, .1),
                new ShortGenerator((short) -6000, (short) 65535, .1),
                new IntGenerator(10, 100, .1),
                new LongGenerator(10, 100, .1),
                // TODO (deephaven-core#4743) verify this change in range
                new FloatGenerator(0, 100, .1),
                new DoubleGenerator(10.1, 20.1, .1),
                new BooleanGenerator(.5, .1),
                new BigIntegerGenerator(new BigInteger("-10"), new BigInteger("10"), .1),
                new BigDecimalGenerator(new BigInteger("1"), new BigInteger("2"), 5, .1)));

        final Random random = new Random(seed);
        final ColumnInfo[] columnInfos = initColumnInfos(colsList.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                generators.toArray(new TestDataGenerator[0]));
        final QueryTable t = getTable(tableSize, random, columnInfos);


        // if (!isRefreshing && includeGroups) {
        // final ColumnSource<?> groupingSource = t.getColumnSource("Sym");
        // groupingSource.setGroupingProvider(StaticGroupingProvider.buildFrom(groupingSource, t.getRowSet()));
        // }

        t.setRefreshing(isRefreshing);

        return new CreateResult(t, columnInfos, random);
    }
}
