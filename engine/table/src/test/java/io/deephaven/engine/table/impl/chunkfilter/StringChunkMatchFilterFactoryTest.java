//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertEquals;

public class StringChunkMatchFilterFactoryTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final String[] VALUES = {"Alpha", "beta", "GAMMA", "delta", "Epsilon"};
    private static final String[] PROBES = {"alpha", "ALPHA", "Beta", "gamma", "DELTA", "epsilon", "zeta", "", null};

    private static boolean expectedMatch(final List<String> values, final String probe) {
        for (final String value : values) {
            if (value == null ? probe == null : value.equalsIgnoreCase(probe)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Every value count is covered, since the factory returns a different filter for one, two, three and more values,
     * and each with and without a null among the values.
     */
    @Test
    public void caseInsensitiveMatchesAgreeWithEqualsIgnoreCase() {
        for (int count = 1; count <= VALUES.length; ++count) {
            for (final boolean withNull : new boolean[] {false, true}) {
                final List<String> values = new ArrayList<>(Arrays.asList(VALUES).subList(0, count));
                if (withNull) {
                    // replace rather than append, so that the null lands in every filter's value slots
                    values.set(count - 1, null);
                }
                for (final boolean inverted : new boolean[] {false, true}) {
                    final MatchOptions options = MatchOptions.builder()
                            .caseInsensitive(true)
                            .inverted(inverted)
                            .build();
                    final ObjectChunkFilter<?> filter =
                            StringChunkMatchFilterFactory.makeCaseInsensitiveFilter(options, values.toArray());
                    for (final String probe : PROBES) {
                        // noinspection unchecked
                        final boolean actual = ((ObjectChunkFilter<String>) filter).matches(probe);
                        assertEquals("values=" + values + ", inverted=" + inverted + ", probe=" + probe,
                                inverted != expectedMatch(values, probe), actual);
                    }
                }
            }
        }
    }

    private static Table data() {
        return newTable(stringCol("S", "a", "B", null));
    }

    @Test
    public void caseInsensitiveInNullAgreesWithCaseSensitive() {
        assertTableEquals(data().where("S in null"), data().where("S icase in null"));
        assertTableEquals(data().where("S not in null"), data().where("S icase not in null"));
    }

    @Test
    public void caseInsensitiveInManyValuesWithNullAgreesWithCaseSensitive() {
        assertTableEquals(
                data().where("S in null, `a`, `x`, `y`, `z`"),
                data().where("S icase in null, `A`, `x`, `y`, `z`"));
        assertTableEquals(
                data().where("S not in null, `a`, `x`, `y`, `z`"),
                data().where("S icase not in null, `A`, `x`, `y`, `z`"));
    }
}
