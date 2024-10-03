//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuppressWarnings({"DataFlowIssue", "ConstantValue"})
public class TestYearMonthSummaryCache extends BaseArrayTestCase {

    private static class Value extends ReadOptimizedConcurrentCache.Pair<String> {
        Value(int key, String value) {
            super(key, value);
        }
    }

    public void testKeys() {
        final int y = 2021;
        final int m = 3;
        final int key = YearMonthSummaryCache.yearMonthKey(y, m);
        assertEquals(key, 2021 * 100 + m);
        assertEquals(y, YearMonthSummaryCache.yearFromYearMonthKey(key));
        assertEquals(m, YearMonthSummaryCache.monthFromYearMonthKey(key));
    }

    public void testGetters() {
        final int[] monthCount = new int[] {0};
        final int[] yearCount = new int[] {0};

        final IntFunction<Value> monthSummary = i -> {
            monthCount[0]++;
            return new Value(i, "month" + i);
        };

        final IntFunction<Value> yearSummary = i -> {
            yearCount[0]++;
            return new Value(i, "year" + i);
        };

        final YearMonthSummaryCache<Value> cache = new YearMonthSummaryCache<>(monthSummary, yearSummary);

        cache.clear();
        monthCount[0] = 0;
        yearCount[0] = 0;

        assertEquals("month202101", cache.getMonthSummary(2021, 1).getValue());
        assertEquals(1, monthCount[0]);
        assertEquals(0, yearCount[0]);
        assertEquals("year2021", cache.getYearSummary(2021).getValue());
        assertEquals(1, monthCount[0]);
        assertEquals(1, yearCount[0]);
        assertEquals("month202101", cache.getMonthSummary(2021, 1).getValue());
        assertEquals(1, monthCount[0]);
        assertEquals(1, yearCount[0]);
        assertEquals("year2021", cache.getYearSummary(2021).getValue());
        assertEquals(1, monthCount[0]);
        assertEquals(1, yearCount[0]);

        assertEquals("month202102", cache.getMonthSummary(2021, 2).getValue());
        assertEquals(2, monthCount[0]);
        assertEquals(1, yearCount[0]);
        assertEquals("year2022", cache.getYearSummary(2022).getValue());
        assertEquals(2, monthCount[0]);
        assertEquals(2, yearCount[0]);

        cache.clear();

        assertEquals("month202101", cache.getMonthSummary(2021, 1).getValue());
        assertEquals(3, monthCount[0]);
        assertEquals(2, yearCount[0]);
        assertEquals("year2021", cache.getYearSummary(2021).getValue());
        assertEquals(3, monthCount[0]);
        assertEquals(3, yearCount[0]);
        assertEquals("month202101", cache.getMonthSummary(2021, 1).getValue());
        assertEquals(3, monthCount[0]);
        assertEquals(3, yearCount[0]);
        assertEquals("year2021", cache.getYearSummary(2021).getValue());
        assertEquals(3, monthCount[0]);
        assertEquals(3, yearCount[0]);

        assertEquals("month202102", cache.getMonthSummary(2021, 2).getValue());
        assertEquals(4, monthCount[0]);
        assertEquals(3, yearCount[0]);
        assertEquals("year2022", cache.getYearSummary(2022).getValue());
        assertEquals(4, monthCount[0]);
        assertEquals(4, yearCount[0]);
    }

    private static <T> Stream<T> iteratorToStream(Iterator<T> iterator) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
        return StreamSupport.stream(spliterator, false);
    }

    public void testIteratorInclusive() {
        final YearMonthSummaryCache<Value> cache =
                new YearMonthSummaryCache<>(i -> new Value(i, "month" + i), i -> new Value(i, "year" + i));
        final boolean startInclusive = true;
        final boolean endInclusive = true;

        // end before start
        LocalDate start = LocalDate.of(2021, 1, 2);
        LocalDate end = LocalDate.of(2021, 1, 1);
        String[] target = {};
        String[] actual =
                iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // same month
        start = LocalDate.of(2021, 1, 1);
        end = LocalDate.of(2021, 1, 11);
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // adjacent partial months
        start = LocalDate.of(2021, 1, 3);
        end = LocalDate.of(2021, 2, 11);
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // full month + partial month
        start = LocalDate.of(2021, 1, 1);
        end = LocalDate.of(2021, 2, 11);
        target = new String[] {"month202101"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // full month + few days
        start = LocalDate.of(2020, 12, 12);
        end = LocalDate.of(2021, 2, 11);
        target = new String[] {"month202101"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // multiple months + few days
        start = LocalDate.of(2020, 11, 12);
        end = LocalDate.of(2021, 4, 11);
        target = new String[] {"month202012", "month202101", "month202102", "month202103"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // partial month + full month
        start = LocalDate.of(2021, 1, 3);
        end = LocalDate.of(2021, 2, 28);
        target = new String[] {"month202102"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // full year
        start = LocalDate.of(2021, 1, 1);
        end = LocalDate.of(2021, 12, 31);
        target = new String[] {"year2021"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // full year + few days
        start = LocalDate.of(2020, 12, 11);
        end = LocalDate.of(2022, 1, 3);
        target = new String[] {"year2021"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // multiple years + few days
        start = LocalDate.of(2018, 12, 11);
        end = LocalDate.of(2022, 1, 3);
        target = new String[] {"year2019", "year2020", "year2021"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // mixed
        start = LocalDate.of(2018, 10, 11);
        end = LocalDate.of(2022, 3, 3);
        target = new String[] {"month201811", "month201812", "year2019", "year2020", "year2021", "month202201",
                "month202202"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);
    }

    public void testIteratorExclusiveInclusive() {
        final YearMonthSummaryCache<Value> cache =
                new YearMonthSummaryCache<>(i -> new Value(i, "month" + i), i -> new Value(i, "year" + i));

        // start and end of month

        LocalDate start = LocalDate.of(2021, 12, 1);
        LocalDate end = LocalDate.of(2021, 12, 31);

        boolean startInclusive = true;
        boolean endInclusive = true;
        String[] target = new String[] {"month202112"};
        String[] actual =
                iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                        .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = true;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = true;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // day before start of month

        start = LocalDate.of(2021, 11, 30);
        end = LocalDate.of(2021, 12, 31);

        startInclusive = true;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = true;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // day after end of month

        start = LocalDate.of(2021, 12, 1);
        end = LocalDate.of(2022, 1, 1);

        startInclusive = true;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = true;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = true;
        endInclusive = false;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        // day before and after end of month

        start = LocalDate.of(2021, 11, 30);
        end = LocalDate.of(2022, 1, 1);

        startInclusive = true;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = true;
        endInclusive = false;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = false;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).map(x -> x.getValue())
                .toArray(String[]::new);
        assertEquals(target, actual);
    }
}
