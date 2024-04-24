//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;

/**
 * A thread-safe lazily initialized cache for year and month summaries.
 *
 * @param <T> the type of the summary
 */
class YearMonthSummaryCache<T> {

    private final FastConcurrentCache<Integer, T> monthCache;
    private final FastConcurrentCache<Integer, T> yearCache;
    private volatile boolean fastCache = false; // synchronized

    /**
     * Creates a new cache.
     *
     * @param computeMonthSummary the function to compute a month summary
     * @param computeYearSummary  the function to compute a year summary
     */
    YearMonthSummaryCache(Function<Integer, T> computeMonthSummary, Function<Integer, T> computeYearSummary) {
        monthCache = new FastConcurrentCache<>(computeMonthSummary);
        yearCache = new FastConcurrentCache<>(computeYearSummary);
    }

    /**
     * Returns whether the fast cache is enabled.
     *
     * @return whether the fast cache is enabled
     */
    public synchronized boolean isFastCache() {
        return fastCache;
    }

    /**
     * Computes the summaries for the specified range and caches them.
     * The map is changed from a ConcurrentHashMap to a HashMap for faster access.
     * This results in faster cache access, but it limits the range of dates in the cache.
     * <p>
     * To enable the fast cache for a different range, clear the cache first.
     *
     * @param startYear  the start year (inclusive)
     * @param startMonth the start month (inclusive)
     * @param endYear    the end year (inclusive)
     * @param endMonth   the end month (inclusive)
     * @param wait       whether to wait for the computation to finish
     *                   before returning
     * @throws IllegalStateException if the fast cache is already enabled
     */
    synchronized void enableFastCache(final int startYear, final int startMonth, final int endYear, final int endMonth, final boolean wait) {

        if (fastCache) {
            throw new IllegalStateException("Fast cache is already enabled.  To change the range, clear the cache first.");
        }

        fastCache = true;

        final ArrayList<Integer> yearMonths = new ArrayList<>();

        for (int year = startYear; year <= endYear; year++) {
            for (int month = (year == startYear ? startMonth : 1); month <= (year == endYear ? endMonth : 12); month++) {
               yearMonths.add(year * 100 + month);
            }
        }

        monthCache.enableFastCache(yearMonths, wait);

        final ArrayList<Integer> years = new ArrayList<>();

        for (int year = (startMonth == 1 ? startYear : startYear + 1); year <= (endMonth == 12 ? endYear : endYear - 1); year++) {
            years.add(year);
        }

        yearCache.enableFastCache(years, wait);
    }

    /**
     * Computes the summaries for the specified range and caches them.
     * The map is changed from a ConcurrentHashMap to a HashMap for faster access.
     * This results in faster cache access, but it limits the range of dates in the cache.
     * <p>
     * To enable the fast cache for a different range, clear the cache first.
     *
     * @param start the start date (inclusive)
     * @param end   the end date (inclusive)
     * @param wait  whether to wait for the computation to finish
     *              before returning
     * @throws IllegalStateException if the fast cache is already enabled
     */
    void enableFastCache(LocalDate start, LocalDate end, final boolean wait) {
        // Ensure only full months are computed

        // Skip the first month if the start date is not the first day of the month
        if (start.getDayOfMonth() != 1) {
            start = start.withDayOfMonth(1).plusMonths(1);
        }

        // Skip the last month if the end date is not the last day of the month

        final LocalDate endPlus1 = end.plusDays(1);

        if (end.getMonth() == endPlus1.getMonth()) {
            end = end.withDayOfMonth(1).minusMonths(1);
        }

        enableFastCache(start.getYear(), start.getMonthValue(), end.getYear(), end.getMonthValue(), wait);
    }

    /**
     * Clears the cache.
     */
    synchronized void clear() {
        fastCache = false;
        monthCache.clear();
        yearCache.clear();
    }

    /**
     * Gets the month summary for the specified year and month.
     *
     * @param yearMonth the year and month
     * @return the month summary
     */
    T getMonthSummary(int yearMonth) {
        return monthCache.get(yearMonth);
    }

    /**
     * Gets the month summary for the specified year and month.
     *
     * @param year  the year
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
        return yearCache.get(year);
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
     * @param start          the start date
     * @param end            the end date
     * @param startInclusive whether the start date is inclusive
     * @param endInclusive   whether the end date is inclusive
     * @return the iterator
     */
    Iterator<T> iterator(final LocalDate start, final LocalDate end,
                         final boolean startInclusive, final boolean endInclusive) {
        return new YearMonthSummaryIterator(startInclusive ? start : start.plusDays(1),
                endInclusive ? end : end.minusDays(1));
    }
}
