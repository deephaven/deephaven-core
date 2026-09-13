//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 19: a case-insensitive match filter whose value list contains {@code null} threw a
 * {@link NullPointerException} on any non-empty table.
 *
 * <pre>
 * t.where("S icase in null, `aa`")
 * // java.lang.NullPointerException:
 * // Cannot invoke "String.equalsIgnoreCase(String)" because "this.value1" is null
 * </pre>
 *
 * <p>
 * {@code StringChunkMatchFilterFactory} specializes one, two and three match values into dedicated chunk filters that
 * compare with {@code storedValue.equalsIgnoreCase(columnValue)} -- the receiver is the <em>filter's</em> value, so a
 * null there throws for every row, regardless of what the column holds. Four or more values take a different path, a
 * {@code KeyedObjectHashSet} over {@code CIStringKey}, whose {@code equalKey} is already null-safe; so the defect was
 * arity-dependent, and {@code icase in} with four values worked while the same filter with three did not.
 *
 * <p>
 * Case-sensitive {@code in} has always matched null against null, so this also made {@code icase} inconsistent with
 * {@code in} on the one input where they must agree.
 */
public class CaseInsensitiveMatchFilterNullTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    /** Seven rows, exactly one null, with case variants so {@code icase} is distinguishable from {@code in}. */
    private static Table table() {
        return TableTools.newTable(TableTools.stringCol("S", "Aa", "aA", "bB", null, "cC", "dD", "eE"));
    }

    // --- Non-inverted: `icase in`, one value per arity of the specialization ---

    @Test
    public void oneValueNull() {
        assertEquals(1, table().where("S icase in null").size());
    }

    @Test
    public void twoValuesWithNull() {
        // null, plus both case variants of "aa".
        assertEquals(3, table().where("S icase in null, `aa`").size());
    }

    @Test
    public void threeValuesWithNull() {
        assertEquals(4, table().where("S icase in null, `aa`, `BB`").size());
    }

    /** Four values already took the null-safe {@code KeyedObjectHashSet} path; it must stay correct. */
    @Test
    public void fourValuesWithNull() {
        assertEquals(5, table().where("S icase in null, `aa`, `BB`, `cc`").size());
    }

    /** null in a non-leading position, in case the fix only covered the first slot. */
    @Test
    public void nullInTrailingPosition() {
        assertEquals(3, table().where("S icase in `aa`, null").size());
        assertEquals(4, table().where("S icase in `aa`, `BB`, null").size());
        assertEquals(4, table().where("S icase in `aa`, null, `BB`").size());
    }

    // --- Inverted: `icase not in` ---

    @Test
    public void invertedOneValueNull() {
        assertEquals(6, table().where("S icase not in null").size());
    }

    @Test
    public void invertedTwoValuesWithNull() {
        assertEquals(4, table().where("S icase not in null, `aa`").size());
    }

    @Test
    public void invertedThreeValuesWithNull() {
        assertEquals(3, table().where("S icase not in null, `aa`, `BB`").size());
    }

    @Test
    public void invertedFourValuesWithNull() {
        assertEquals(2, table().where("S icase not in null, `aa`, `BB`, `cc`").size());
    }

    /**
     * Repeated nulls, which is what the fuzzer generated: {@code Col1 icase not in null, null, null} reached the
     * three-value specialization with every slot null.
     */
    @Test
    public void repeatedNulls() {
        assertEquals(1, table().where("S icase in null, null").size());
        assertEquals(1, table().where("S icase in null, null, null").size());
        assertEquals(6, table().where("S icase not in null, null, null").size());
    }

    /** An all-null column, so the match value and every column value are null. */
    @Test
    public void allNullColumn() {
        final Table allNull = TableTools.newTable(TableTools.stringCol("S", null, null, null));
        assertEquals(3, allNull.where("S icase in null").size());
        assertEquals(3, allNull.where("S icase in null, `aa`").size());
        assertEquals(0, allNull.where("S icase not in null").size());
    }

    /**
     * The consistency property, on value lists that are all null: {@code icase} and case-sensitive {@code in} must
     * agree exactly, since case folding cannot distinguish null from anything else.
     */
    @Test
    public void agreesWithCaseSensitiveOnNullOnlyValues() {
        for (final String values : new String[] {"null", "null, null", "null, null, null",
                "null, null, null, null"}) {
            assertEquals("in " + values,
                    table().where("S in " + values).size(),
                    table().where("S icase in " + values).size());
            assertEquals("not in " + values,
                    table().where("S not in " + values).size(),
                    table().where("S icase not in " + values).size());
        }
    }

    /**
     * With non-null values alongside the null, the two filters disagree by design -- {@code icase} also matches the
     * other case variants -- so only the null row's treatment is comparable, and that is the part that was broken.
     * Checked at every arity, since the defect was arity-dependent.
     */
    @Test
    public void nullIsMatchedAlongsideNonNullValues() {
        for (final String values : new String[] {"null, `aA`", "null, `aA`, `bB`", "null, `aA`, `bB`, `cC`"}) {
            assertEquals("in " + values, 1,
                    table().where("S icase in " + values).where("S == null").size());
            assertEquals("not in " + values, 0,
                    table().where("S icase not in " + values).where("S == null").size());
        }
    }

    /** No null in the value list: the ordinary path, which must be untouched. */
    @Test
    public void noNullValuesUnaffected() {
        assertEquals(2, table().where("S icase in `aa`").size());
        assertEquals(3, table().where("S icase in `aa`, `bb`").size());
        assertEquals(4, table().where("S icase in `aa`, `bb`, `cc`").size());
        assertEquals(5, table().where("S icase in `aa`, `bb`, `cc`, `dd`").size());
        assertEquals(5, table().where("S icase not in `aa`").size());
    }
}
