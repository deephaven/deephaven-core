//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuppressWarnings({"DataFlowIssue", "ConstantValue"})
public class TestYearMonthSummaryCache extends BaseArrayTestCase {

    public void testGetters() {
        final int[] monthCount = new int[] {0};
        final int[] yearCount = new int[] {0};

        final Function<Integer, String> monthSummary = i -> {
            monthCount[0]++;
            return "month" + i;
        };

        final Function<Integer, String> yearSummary = i -> {
            yearCount[0]++;
            return "year" + i;
        };

        final YearMonthSummaryCache<String> cache = new YearMonthSummaryCache<>(monthSummary, yearSummary);
        assertEquals("month202101", cache.getMonthSummary(202101));
        assertEquals(1, monthCount[0]);
        assertEquals(0, yearCount[0]);
        assertEquals("year2021", cache.getYearSummary(2021));
        assertEquals(1, monthCount[0]);
        assertEquals(1, yearCount[0]);
        assertEquals("month202101", cache.getMonthSummary(202101));
        assertEquals(1, monthCount[0]);
        assertEquals(1, yearCount[0]);
        assertEquals("year2021", cache.getYearSummary(2021));
        assertEquals(1, monthCount[0]);
        assertEquals(1, yearCount[0]);

        assertEquals("month202102", cache.getMonthSummary(202102));
        assertEquals(2, monthCount[0]);
        assertEquals(1, yearCount[0]);
        assertEquals("year2022", cache.getYearSummary(2022));
        assertEquals(2, monthCount[0]);
        assertEquals(2, yearCount[0]);

        cache.clear();

        assertEquals("month202101", cache.getMonthSummary(202101));
        assertEquals(3, monthCount[0]);
        assertEquals(2, yearCount[0]);
        assertEquals("year2021", cache.getYearSummary(2021));
        assertEquals(3, monthCount[0]);
        assertEquals(3, yearCount[0]);
        assertEquals("month202101", cache.getMonthSummary(202101));
        assertEquals(3, monthCount[0]);
        assertEquals(3, yearCount[0]);
        assertEquals("year2021", cache.getYearSummary(2021));
        assertEquals(3, monthCount[0]);
        assertEquals(3, yearCount[0]);

        assertEquals("month202102", cache.getMonthSummary(202102));
        assertEquals(4, monthCount[0]);
        assertEquals(3, yearCount[0]);
        assertEquals("year2022", cache.getYearSummary(2022));
        assertEquals(4, monthCount[0]);
        assertEquals(4, yearCount[0]);

        assertEquals(cache.getMonthSummary(202101), cache.getMonthSummary(2021, 1));
    }

    private static <T> Stream<T> iteratorToStream(Iterator<T> iterator) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
        return StreamSupport.stream(spliterator, false);
    }

    public void testIteratorInclusive() {
        final YearMonthSummaryCache<String> cache = new YearMonthSummaryCache<>(i -> "month" + i, i -> "year" + i);
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
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // full month + few days
        start = LocalDate.of(2020, 12, 12);
        end = LocalDate.of(2021, 2, 11);
        target = new String[] {"month202101"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // multiple months + few days
        start = LocalDate.of(2020, 11, 12);
        end = LocalDate.of(2021, 4, 11);
        target = new String[] {"month202012", "month202101", "month202102", "month202103"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // partial month + full month
        start = LocalDate.of(2021, 1, 3);
        end = LocalDate.of(2021, 2, 28);
        target = new String[] {"month202102"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // full year
        start = LocalDate.of(2021, 1, 1);
        end = LocalDate.of(2021, 12, 31);
        target = new String[] {"year2021"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // full year + few days
        start = LocalDate.of(2020, 12, 11);
        end = LocalDate.of(2022, 1, 3);
        target = new String[] {"year2021"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // multiple years + few days
        start = LocalDate.of(2018, 12, 11);
        end = LocalDate.of(2022, 1, 3);
        target = new String[] {"year2019", "year2020", "year2021"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // mixed
        start = LocalDate.of(2018, 10, 11);
        end = LocalDate.of(2022, 3, 3);
        target = new String[] {"month201811", "month201812", "year2019", "year2020", "year2021", "month202201",
                "month202202"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);
    }

    public void testIteratorExclusiveInclusive() {
        final YearMonthSummaryCache<String> cache = new YearMonthSummaryCache<>(i -> "month" + i, i -> "year" + i);

        // start and end of month

        LocalDate start = LocalDate.of(2021, 12, 1);
        LocalDate end = LocalDate.of(2021, 12, 31);

        boolean startInclusive = true;
        boolean endInclusive = true;
        String[] target = new String[] {"month202112"};
        String[] actual =
                iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = true;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = true;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // day before start of month

        start = LocalDate.of(2021, 11, 30);
        end = LocalDate.of(2021, 12, 31);

        startInclusive = true;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = true;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // day after end of month

        start = LocalDate.of(2021, 12, 1);
        end = LocalDate.of(2022, 1, 1);

        startInclusive = true;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = true;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = true;
        endInclusive = false;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = false;
        target = new String[] {};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        // day before and after end of month

        start = LocalDate.of(2021, 11, 30);
        end = LocalDate.of(2022, 1, 1);

        startInclusive = true;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = true;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = true;
        endInclusive = false;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);

        startInclusive = false;
        endInclusive = false;
        target = new String[] {"month202112"};
        actual = iteratorToStream(cache.iterator(start, end, startInclusive, endInclusive)).toArray(String[]::new);
        assertEquals(target, actual);
    }
}
