//
// Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A thread-safe lazily initialized cache for year and month summaries.
 *
 * @param <T> the type of the summary
 */
class YearMonthSummaryCache<T> {

    private final Map<Integer, T> yearCache = new ConcurrentHashMap<>();
    private final Map<Integer, T> monthCache = new ConcurrentHashMap<>();

    private final Function<Integer, T> computeMonthSummary;
    private final Function<Integer, T> computeYearSummary;

    /**
     * Creates a new cache.
     *
     * @param computeMonthSummary the function to compute a month summary
     * @param computeYearSummary the function to compute a year summary
     */
    YearMonthSummaryCache(Function<Integer, T> computeMonthSummary, Function<Integer, T> computeYearSummary) {
        this.computeMonthSummary = computeMonthSummary;
        this.computeYearSummary = computeYearSummary;
    }

    /**
     * Clears the cache.
     */
    void clear() {
        yearCache.clear();
        monthCache.clear();
    }

    /**
     * Gets the month summary for the specified year and month.
     *
     * @param yearMonth the year and month
     * @return the month summary
     */
    T getMonthSummary(int yearMonth) {
        return monthCache.computeIfAbsent(yearMonth, computeMonthSummary);
    }

    /**
     * Gets the month summary for the specified year and month.
     *
     * @param year the year
     * @param month the month
     * @return the month summary
     */
    T getMonthSummary(int year, int month) {
        return getMonthSummary(year * 100 + month);
    }

    /**
     * Gets the year summary for the specified year.
     *
     * @param year the year
     * @return the year summary
     */
    T getYearSummary(int year) {
        return yearCache.computeIfAbsent(year, computeYearSummary);
    }

    private class YearMonthSummaryIterator implements Iterator<T> {

        private int currentYear;
        private int currentMonth;
        private int currentYearMonth;
        private int finalYear;
        private int finalMonth;
        final private int finalYearMonth;

        YearMonthSummaryIterator(LocalDate start, LocalDate end) {
            int startYear = start.getYear();
            int startMonth = start.getMonthValue();
            int endYear = end.getYear();
            int endMonth = end.getMonthValue();

            currentMonth = startMonth;
            currentYear = startYear;

            if (start.getDayOfMonth() != 1) {
                incrementCurrentByMonth();
            }

            currentYearMonth = currentYear * 100 + currentMonth;

            final LocalDate endPlus1 = end.plusDays(1);
            final int endPlus1Month = endPlus1.getMonthValue();

            finalMonth = endMonth;
            finalYear = endYear;

            if (endPlus1Month == endMonth) {
                if (finalMonth == 1) {
                    finalMonth = 12;
                    finalYear = finalYear - 1;
                } else {
                    finalMonth = finalMonth - 1;
                }
            }

            finalYearMonth = finalYear * 100 + finalMonth;
        }

        private void incrementCurrentByMonth() {
            if (currentMonth == 12) {
                currentMonth = 1;
                currentYear += 1;
            } else {
                currentMonth = currentMonth + 1;
            }

            currentYearMonth = currentYear * 100 + currentMonth;
        }

        private void incrementCurrentByYear() {
            currentYear++;
            currentYearMonth = currentYear * 100 + currentMonth;
        }

        @Override
        public boolean hasNext() {
            return currentYearMonth <= finalYearMonth;
        }

        @Override
        public T next() {
            final T val;

            if (currentMonth == 1 && (currentYear != finalYear || finalMonth == 12)) {
                val = getYearSummary(currentYear);
                incrementCurrentByYear();
            } else {
                val = getMonthSummary(currentYear * 100 + currentMonth);
                incrementCurrentByMonth();
            }

            return val;
        }
    }

    /**
     * Gets an iterator over the summaries for the specified range. The returned iterator will include the start date if
     * {@code startInclusive} is true, and the end date if {@code endInclusive} is true. If the start date is after the
     * end date, the iterator will be empty.
     * <p>
     * The iterator will return summaries in chronological order, and these summaries can be a mix of month and year
     * summaries. Dates not represented by complete summaries will be skipped (e.g. partial months).
     *
     * @param start the start date
     * @param end the end date
     * @param startInclusive whether the start date is inclusive
     * @param endInclusive whether the end date is inclusive
     * @return the iterator
     */
    Iterator<T> iterator(final LocalDate start, final LocalDate end,
            final boolean startInclusive, final boolean endInclusive) {
        return new YearMonthSummaryIterator(startInclusive ? start : start.plusDays(1),
                endInclusive ? end : end.minusDays(1));
    }
}
