package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterPattern.Mode;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;

import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class FilterPatternTest extends RefreshingTableTestCase {

    public static final ColumnName COLUMN = ColumnName.of("S");

    private static String[] regex() {
        return new String[] {
                ".",
                ".*",
                "foo",
                "bar",
                "baz",
                "^foo",
                "^bar",
                "foo$",
                "bar$",
                ".*foo.*",
                ".*bar.*",
                "^foo.*",
                "^bar.*",
                ".*foo$",
                ".*bar$",
                Pattern.quote("\"foo\"")
        };
    }

    private static String[] data() {
        final String[] examples = {
                "",
                " ",
                "  ",
                "foo",
                "foobar",
                "foo bar",
                "food burritto",
                "BAR FOOD",
                " foo",
                " bar",
                "foo ",
                "bar ",
                "*",
                ".",
                "\"foo\"",
                "\"bar\"",
                "\"foo bar\"",
                "\"foo\" \"bar\"",
                "\nfoo",
                "foo\n",
                "\nfoo\n",
                "fo oo",
                "f00",
                "FoO",
                "BaR",
        };
        // mixin other data / modifications
        return Stream.of(
                Stream.of(examples),
                Stream.of(examples).map(Pattern::quote),
                Stream.of(regex()),
                Stream.of(regex()).map(Pattern::quote))
                .flatMap(Function.identity())
                .toArray(String[]::new);
    }

    private static int[] flags() {
        return new int[] {
                0,
                Pattern.CASE_INSENSITIVE,
                Pattern.LITERAL,
                Pattern.DOTALL,
                Pattern.MULTILINE,
                Pattern.CASE_INSENSITIVE | Pattern.LITERAL,
                Pattern.DOTALL | Pattern.MULTILINE,
        };
    }

    public void testMatches() {
        final String[] data = data();
        for (String regex : regex()) {
            for (int flag : flags()) {
                final FilterPattern matches = FilterPattern.of(COLUMN, Pattern.compile(regex, flag), Mode.MATCHES);
                test(matches, data);
            }
        }
    }

    public void testFind() {
        final String[] data = data();
        for (String regex : regex()) {
            for (int flag : flags()) {
                final FilterPattern matches = FilterPattern.of(COLUMN, Pattern.compile(regex, flag), Mode.FIND);
                test(matches, data);
            }
        }
    }

    private static void test(FilterPattern pattern, String[] data) {
        final Table table = TableTools.newTable(TableTools.stringCol(COLUMN.name(), data));
        final TrackingWritableRowSet[] expected = build(pattern, data);
        final Table expectedYes = table.getSubTable(expected[0]);
        final Table expectedNo = table.getSubTable(expected[1]);
        final Table yes = table.where(pattern);
        final Table no = table.where(Filter.not(pattern));
        TstUtils.assertTableEquals(expectedYes, yes);
        TstUtils.assertTableEquals(expectedNo, no);
    }

    private static TrackingWritableRowSet[] build(FilterPattern pattern, String[] data) {
        final RowSetBuilderSequential yes = RowSetFactory.builderSequential();
        final RowSetBuilderSequential no = RowSetFactory.builderSequential();
        for (int i = 0; i < data.length; i++) {
            if (test(pattern, data[i])) {
                yes.appendKey(i);
            } else {
                no.appendKey(i);
            }
        }
        return new TrackingWritableRowSet[] {yes.build().toTracking(), no.build().toTracking()};
    }

    private static boolean test(FilterPattern pattern, CharSequence input) {
        // This isn't the most robust of test setups - but it is divorced from all of the actual engine implementation
        // and is easy to express our expected logic here.
        switch (pattern.mode()) {
            case FIND:
                return pattern.pattern().matcher(input).find();
            case MATCHES:
                return pattern.pattern().matcher(input).matches();
            default:
                throw new IllegalStateException();
        }
    }
}
