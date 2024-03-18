//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.select.ReinterpretedColumn;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * This class contains static methods to support conversions between various time types such as {@link Instant},
 * {@link ZonedDateTime}, {@link LocalDate}, {@link LocalTime}, and {@code long}.
 *
 * <p>
 * For example, lets say that you wanted to select multiple days from a table, but filter them to a specific times of
 * the day.
 * </p>
 *
 * <pre>
 * baseTable = db.i("Market", "Trades")
 *               .where("Date &gt; 2021-10-01")
 *
 * startTime = LocalTime.of(10,30,00)
 * endTime = LocalTime.of(16,30,00)
 * augmented = TimeTableConversions.asLocalTime(baseTable, "LocalTime = Timestamp", "America/New_York"))
 *                                 .where("LocalTime.isAfter(startTime)", "LocalTime.isBefore(endTime)")
 * </pre>
 */
public class TableTimeConversions {
    @NotNull
    private static Table convertTimeColumn(@NotNull final Table source, @NotNull final MatchPair mp,
            @NotNull final Class<?> resultType, Object... params) {
        final ColumnSource<?> cd = Require.neqNull(source, "source").getColumnSource(mp.rightColumn);
        final Class<?> colType = cd.getType();

        // We cannot simply return the source if we are converting between types that require a zone because we don't
        // know what the time zones are compared to the current one. so we have to reinterpret and potentially switch
        // zones.
        if (colType == resultType && mp.leftColumn.equals(mp.rightColumn)
                && (!requiresZone(resultType) || !requiresZone(colType))) {
            return source;
        }

        return source.updateView(
                List.of(new ReinterpretedColumn<>(mp.rightColumn, colType, mp.leftColumn, resultType, params)));
    }

    // region To ZonedDateTime
    /**
     * Convert the specified column in the table to a {@link ZonedDateTime} column at the specified time zone. The
     * column may be specified as a single value "Column" or a pair "NewColumn = OriginalColumn"
     *
     * @param source The source table
     * @param column The column to convert, in {@link MatchPair} format
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link ZonedDateTime}.
     */
    @ScriptApi
    public static Table asZonedDateTime(
            @NotNull final Table source,
            @NotNull final String column,
            @NotNull final String zone) {
        return asZonedDateTime(source,
                MatchPairFactory.getExpression(Require.neqNull(column, "column")),
                ZoneId.of(Require.neqNull(zone, "zone")));
    }

    /**
     * Convert the specified column in the table to a {@link ZonedDateTime} column at the specified time zone. The
     * column may be specified as a single value "Column" or a pair "NewColumn = OriginalColumn"
     *
     * @param source The source table
     * @param matchPair The {@link MatchPair} of columns
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link ZonedDateTime}.
     */
    @ScriptApi
    public static Table asZonedDateTime(
            @NotNull final Table source,
            @NotNull final MatchPair matchPair,
            @NotNull final String zone) {
        return asZonedDateTime(source, matchPair, ZoneId.of(Require.neqNull(zone, "zone")));
    }

    /**
     * Convert the specified column in the table to a {@link ZonedDateTime} column at the specified time zone.
     *
     * @param source The source table
     * @param column The column to convert, in {@link MatchPair} format
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link ZonedDateTime}.
     */
    @ScriptApi
    public static Table asZonedDateTime(
            @NotNull final Table source,
            @NotNull final String column,
            @NotNull final ZoneId zone) {
        return asZonedDateTime(source, MatchPairFactory.getExpression(Require.neqNull(column, "column")), zone);
    }

    /**
     * Convert the specified column in the table to a {@link ZonedDateTime} column at the specified time zone.
     *
     * @param source The source table
     * @param matchPair The {@link MatchPair} of columns
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link ZonedDateTime}.
     */
    @ScriptApi
    public static Table asZonedDateTime(
            @NotNull final Table source,
            @NotNull final MatchPair matchPair,
            @NotNull final ZoneId zone) {
        return convertTimeColumn(source, matchPair, ZonedDateTime.class, zone);
    }

    // endregion

    // region To LocalTime
    /**
     * Convert the specified column in the table to a {@link LocalTime} column at the specified time zone. The column
     * may be specified as a single value "Column" or a pair "NewColumn = OriginalColumn"
     *
     * @param source The source table
     * @param column The column to convert, in {@link MatchPair} format
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link LocalTime}.
     */
    @ScriptApi
    public static Table asLocalTime(
            @NotNull final Table source,
            @NotNull final String column,
            @NotNull final String zone) {
        return asLocalTime(source,
                MatchPairFactory.getExpression(Require.neqNull(column, "column")),
                ZoneId.of(Require.neqNull(zone, "zone")));
    }

    /**
     * Convert the specified column in the table to a {@link LocalTime} column at the specified time zone. The column
     * may be specified as a single value "Column" or a pair "NewColumn = OriginalColumn"
     *
     * @param source The source table
     * @param column The column to convert, in {@link MatchPair} format
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link LocalTime}.
     */
    @ScriptApi
    public static Table asLocalTime(
            @NotNull final Table source,
            @NotNull final String column,
            @NotNull final ZoneId zone) {
        return asLocalTime(source, MatchPairFactory.getExpression(Require.neqNull(column, "column")), zone);
    }

    /**
     * Convert the specified column in the table to a {@link LocalTime} column at the specified time zone.
     *
     * @param source The source table
     * @param matchPair The {@link MatchPair} of columns
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link LocalTime}.
     */
    @ScriptApi
    public static Table asLocalTime(
            @NotNull final Table source,
            @NotNull final MatchPair matchPair,
            @NotNull final String zone) {
        return asLocalTime(source, matchPair, ZoneId.of(Require.neqNull(zone, "zone")));
    }

    /**
     * Convert the specified column in the table to a {@link LocalTime} column at the specified time zone.
     *
     * @param source The source table
     * @param matchPair The {@link MatchPair} of columns
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link LocalTime}.
     */
    @ScriptApi
    public static Table asLocalTime(
            @NotNull final Table source,
            @NotNull final MatchPair matchPair,
            @NotNull final ZoneId zone) {
        return convertTimeColumn(source, matchPair, LocalTime.class, zone);
    }
    // endregion

    // region To LocalDate
    /**
     * Convert the specified column in the table to a {@link LocalDate} column at the specified time zone. The column
     * may be specified as a single value "Column" or a pair "NewColumn = OriginalColumn"
     *
     * @param source The source table
     * @param column The column to convert, in {@link MatchPair} format
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link LocalDate}.
     */
    @ScriptApi
    public static Table asLocalDate(
            @NotNull final Table source,
            @NotNull final String column,
            @NotNull final String zone) {
        return asLocalDate(source,
                MatchPairFactory.getExpression(Require.neqNull(column, "column")),
                ZoneId.of(Require.neqNull(zone, "zone")));
    }

    /**
     * Convert the specified column in the table to a {@link LocalDate} column at the specified time zone. The column
     * may be specified as a single value "Column" or a pair "NewColumn = OriginalColumn"
     *
     * @param source The source table
     * @param column The column to convert, in {@link MatchPair} format
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link LocalDate}.
     */
    @ScriptApi
    public static Table asLocalDate(
            @NotNull final Table source,
            @NotNull final String column,
            @NotNull final ZoneId zone) {
        return asLocalDate(source, MatchPairFactory.getExpression(Require.neqNull(column, "column")), zone);
    }

    /**
     * Convert the specified column in the table to a {@link LocalDate} column at the specified time zone.
     *
     * @param source The source table
     * @param matchPair The {@link MatchPair} of columns
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link LocalDate}.
     */
    @ScriptApi
    public static Table asLocalDate(
            @NotNull final Table source,
            @NotNull final MatchPair matchPair,
            @NotNull final String zone) {
        return asLocalDate(source, matchPair, ZoneId.of(Require.neqNull(zone, "zone")));
    }

    /**
     * Convert the specified column in the table to a {@link LocalDate} column at the specified time zone.
     *
     * @param source The source table
     * @param matchPair The {@link MatchPair} of columns
     * @param zone The time zone to use.
     * @return the {@link Table} with the specified column converted to {@link LocalDate}.
     */
    @ScriptApi
    public static Table asLocalDate(
            @NotNull final Table source,
            @NotNull final MatchPair matchPair,
            @NotNull final ZoneId zone) {
        return convertTimeColumn(source, matchPair, LocalDate.class, zone);
    }
    // endregion

    // region to Instant
    /**
     * Convert the specified column in the table to an {@link Instant} column. The column may be specified as a single
     * value "Column" or a pair "NewColumn = OriginalColumn"
     *
     * @param source The source table
     * @param column The column to convert, in {@link MatchPair} format
     * @return the {@link Table} with the specified column converted to {@link Instant}.
     */
    public static Table asInstant(@NotNull final Table source, @NotNull final String column) {
        return asInstant(source, MatchPairFactory.getExpression(Require.neqNull(column, "column")));
    }

    /**
     * Convert the specified column in the table to an {@link Instant} column.
     *
     * @param source The source table
     * @param matchPair The {@link MatchPair} of columns
     * @return the {@link Table} with the specified column converted to {@link Instant}.
     */
    public static Table asInstant(@NotNull final Table source, @NotNull final MatchPair matchPair) {
        return convertTimeColumn(source, matchPair, Instant.class);
    }
    // endregion

    // region to EpochNanos
    /**
     * Convert the specified column in the table to a {@code long} column of nanos since epoch. The column may be
     * specified as a single value "Column" or a pair "NewColumn = OriginalColumn"
     *
     * @param source The source table
     * @param column The column to convert, in {@link MatchPair} format
     * @return the {@link Table} with the specified column converted to {@code long}.
     */
    public static Table asEpochNanos(@NotNull final Table source, @NotNull final String column) {
        return asEpochNanos(source, MatchPairFactory.getExpression(Require.neqNull(column, "column")));
    }

    /**
     * Convert the specified column in the table to a {@code long} column of nanos since epoch.*
     *
     * @param source The source table
     * @param matchPair The {@link MatchPair} of columns
     * @return the {@link Table} with the specified column converted to {@code long}.
     */
    public static Table asEpochNanos(@NotNull final Table source, @NotNull final MatchPair matchPair) {
        return convertTimeColumn(source, matchPair, long.class);
    }
    // endregion

    /**
     * Check if the supplied type is one of the supported time types.
     *
     * @param type the type
     * @return true if the type is one of the useable time types
     */
    public static boolean isTimeType(@NotNull final Class<?> type) {
        return type == Instant.class
                || type == ZonedDateTime.class
                || type == LocalDate.class
                || type == LocalTime.class;
    }

    /**
     * Check if the supplied time type requires a time zone for construction.
     *
     * @param type the type
     * @return true if the type requires a timezone
     */
    public static boolean requiresZone(@NotNull final Class<?> type) {
        return type == ZonedDateTime.class || type == LocalDate.class || type == LocalTime.class;
    }
}
