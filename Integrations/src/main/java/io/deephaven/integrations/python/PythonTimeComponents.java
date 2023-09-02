package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;

/**
 * Utility functions for {@code time.py} to extract {@link java.time} components in a single call.
 */
@ScriptApi
public class PythonTimeComponents {

    /**
     * Extracts the components from a {@link LocalTime}. Equivalent to {@code new int[] {dt.getHour(), dt.getMinute(),
     * dt.getSecond(), dt.getNano()}}.
     *
     * @param dt the local time
     * @return the components
     */
    @ScriptApi
    public static int[] getComponents(LocalTime dt) {
        return new int[] {dt.getHour(), dt.getMinute(), dt.getSecond(), dt.getNano()};
    }

    /**
     * Extracts the components from a {@link ZonedDateTime}. Equivalent to {@code new int[] {dt.getHour(),
     * dt.getMinute(), dt.getSecond(), dt.getNano()}}.
     *
     * @param dt the zoned date time
     * @return the components
     */
    @ScriptApi
    public static int[] getComponents(ZonedDateTime dt) {
        return new int[] {dt.getHour(), dt.getMinute(), dt.getSecond(), dt.getNano()};
    }

    /**
     * Extracts the components from an {@link Instant}. Equivalent to {@code new long[] {dt.getEpochSecond(),
     * dt.getNano()}}.
     *
     * @param dt the instant
     * @return the components
     */
    @ScriptApi
    public static long[] getComponents(Instant dt) {
        return new long[] {dt.getEpochSecond(), dt.getNano()};
    }

    /**
     * Extracts the {@link Instant} components from a {@link ZonedDateTime}. Equivalent to
     * {@code getComponents(dt.toInstant())}.
     *
     * @param dt the zoned date time
     * @return the components
     * @see #getComponents(Instant)
     * @see ZonedDateTime#toInstant()
     */
    @ScriptApi
    public static long[] getComponentsInstant(ZonedDateTime dt) {
        // return getComponents(dt.toInstant());
        return new long[] {dt.toEpochSecond(), dt.getNano()};
    }

    /**
     * Extracts the components from a {@link Duration}. Equivalent to {@code new long[] {dt.getSeconds(),
     * dt.getNano()}}.
     *
     * @param dt the duration
     * @return the components
     */
    @ScriptApi
    public static long[] getComponents(Duration dt) {
        return new long[] {dt.getSeconds(), dt.getNano()};
    }

    /**
     * Extracts the components from a {@link Period}. Equivalent to {@code new int[] {dt.getYears(), dt.getMonths(),
     * dt.getDays()}}.
     *
     * @param dt the period
     * @return the components
     */
    @ScriptApi
    public static int[] getComponents(Period dt) {
        return new int[] {dt.getYears(), dt.getMonths(), dt.getDays()};
    }

    /**
     * Extracts the components from a {@link LocalDate}. Equivalent to {@code new int[] {dt.getYear(),
     * dt.getMonthValue(), dt.getDayOfMonth()}}.
     *
     * @param dt the local date
     * @return the components
     */
    @ScriptApi
    public static int[] getComponents(LocalDate dt) {
        return new int[] {dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth()};
    }

    /**
     * Extracts the {@link LocalDate} components from a {@link ZonedDateTime}. Equivalent to
     * {@code getComponents(dt.toLocalDate())}.
     *
     * @param dt the zoned date time
     * @return the components
     * @see #getComponents(LocalDate)
     * @see ZonedDateTime#toLocalDate()
     */
    @ScriptApi
    public static int[] getComponentsLocalDate(ZonedDateTime dt) {
        return getComponents(dt.toLocalDate());
    }
}
