//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jdbc;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.jdbc.util.ArrayParser;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class JdbcTypeMapper {

    private static final long MILLIS_TO_NANOS = 1000_000L;

    /**
     * A catch-all bucket for settings used when converting Deephaven column values to and from JDBC values. Storing
     * per-import and per-session information here instead of in the mappers allows the mappers to remain static
     * singletons.
     */
    public static class Context {
        private final Calendar sourceCalendar;
        private final ArrayParser arrayParser;
        private final boolean strict;

        private Context(TimeZone sourceTimeZone, String arrayDelimiter, boolean strict) {
            this.sourceCalendar = Calendar.getInstance(sourceTimeZone);
            this.arrayParser = ArrayParser.getInstance(arrayDelimiter);
            this.strict = strict;
        }


        private Context(Calendar calendar) {
            this.sourceCalendar = calendar;
            this.arrayParser = ArrayParser.getInstance(",");
            this.strict = true;
        }

        /**
         * Create a new context with the given settings.
         *
         * @param sourceTimeZone necessary when dealing with many date/time related operations.
         * @param arrayDelimiter necessary when encoding/decoding arrays as strings
         * @param strict indicates how forgiving to be with malformed/unexpected input data
         */
        public static Context of(TimeZone sourceTimeZone, String arrayDelimiter, boolean strict) {
            return new Context(sourceTimeZone, arrayDelimiter, strict);
        }

        /**
         * Create a new context with the given Calendar. Array delimiter will default to a comma, and strict true.
         *
         * @param calendar necessary when dealing with many date/time related operations.
         *
         */
        public static Context of(Calendar calendar) {
            return new Context((Calendar) calendar.clone());
        }

        /**
         * Get the calendar to use when interpreting local date/time values.
         *
         * @return the calendar
         */
        public Calendar getSourceCalendar() {
            return sourceCalendar;
        }

        /**
         * Get the array delimiter to use when encoding/decoding arrays as String values.
         *
         * @return the delimiter
         */
        public ArrayParser getArrayParser() {
            return arrayParser;
        }

        /**
         * Get the strict setting.
         *
         * @return if conversions should be strict
         */
        public boolean isStrict() {
            return strict;
        }
    }

    /**
     * An abstraction for mapping a JDBC type to a Deephaven column type. Each implementation of DataTypeMapping
     * provides the logic for extracting a value of a specific SQL type from a ResultSet and converting to the Deephaven
     * type as well as binding a Deephaven value to a JDBC Statement appropriately (bidirectional mapping).
     *
     * <p>
     * Note that more than one type mapping is possible for each SQL type - a DATE might be converted to either a
     * LocalDate oran Instant for example. And the same is true for the target type -an Instant might be sourced from a
     * SQL DATE or TIMESTAMP value.
     * </p>
     *
     * <p>
     * Most mappings can lose information, making them "asymmetrical". A trivial example is integer values - since null
     * integers are represented in Deephaven using Integer.MIN_VALUE, an integer value extracted from a JDBC data source
     * with this value will appear as NULL in Deephaven. If this value is then written out to a JDBC data source using
     * the default mapping behavior, it will be mapped to NULL in JDBC, not the original value. This problem is
     * impossible to avoid when the domain of the SQL and Deephaven types do not match precisely.
     * </p>
     *
     * <p>
     * Instant values have a different problem - SQL and Deephaven Instant values are conceptually different. TIMESTAMP
     * columns in SQL databases typically do not store timezone information. They are equivalent to a java LocalDateTime
     * in this respect. However, Deephaven usually stores timestamps in Instant columns, internally represented as
     * nanos-since-the-epoch. This means JDBC timestamps usually require the "context" of a timezone in order to be
     * converted toan Instant properly. Writingan Instant to a JDBC datasource (via the bind mechanism) has the same
     * issue. Unfortunately this time-zone context issue applies even when mapping JDBC DATETIME values "directly" to
     * LocalDate, because most JDBC drivers require all Date related values to pass through a java.sql.Date object,
     * which is also epoch-based, not "local". The latest JDBC standard deals with this problem but doesn't seem to be
     * widely implemented. Here we handle this problem by passing a "Context" object every time a conversion occurs,
     * which contains the timezone to be used when extracting or binding JDBC date or timestamp values.
     * </p>
     *
     * @param <T> the Deephaven column type this mapping handles
     */
    public static abstract class DataTypeMapping<T> {
        final int sqlType;
        private final Class<T> dbType;
        private final Class<?> inputType;

        DataTypeMapping(int sqlType, @NotNull final Class<T> type) {
            this(sqlType, type, type);
        }

        DataTypeMapping(int sqlType, @NotNull final Class<T> dbType, @NotNull final Class<?> inputType) {
            this.sqlType = sqlType;
            this.dbType = dbType;
            this.inputType = inputType;
        }

        public Class<T> getDeephavenType() {
            return dbType;
        }

        public Class<?> getInputType() {
            return inputType;
        }

        /**
         * Get a value from the current row in the given ResultSet, convert to the target type, insert into destination
         * chunk. A Context object is provided for additional context or "settings" regarding how to perform the
         * conversion (for example, the source time zone).
         *
         * @param destChunk the chunk to write to
         * @param destOffset the location in the chunk to write to
         * @param resultSet from which to extract the value
         * @param columnIndex ResultSet column from which to extract the value (1-based)
         * @param context conversion context information
         * @throws SQLException if an error occurs
         */
        public abstract void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException;


        /**
         * Bind the given value from the chunk in the given prepared statement.
         *
         * @param srcChunk the chunk to read from
         * @param srcOffset the location in the chunk to read from
         * @param stmt statement to which to bind the given value
         * @param parameterIndex parameter index to bind
         * @param context context information for the binding
         * @throws SQLException if an error occurs
         */
        public abstract void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException;
    }

    public static class StringDataTypeMapping extends DataTypeMapping<String> {
        StringDataTypeMapping(int sqlType) {
            super(sqlType, String.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<String, Values> objectChunk = destChunk.asWritableObjectChunk();
            objectChunk.set(destOffset, resultSet.getString(columnIndex));
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<String, Values> objectChunk = srcChunk.asObjectChunk();
            final String value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setString(parameterIndex, value);
            }
        }
    }

    public static class ByteArrayDataTypeMapping extends DataTypeMapping<byte[]> {
        ByteArrayDataTypeMapping(int sqlType) {
            super(sqlType, byte[].class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<byte[], Values> objectChunk = destChunk.asWritableObjectChunk();
            objectChunk.set(destOffset, resultSet.getBytes(columnIndex));
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<byte[], Values> objectChunk = srcChunk.asObjectChunk();
            final byte[] value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setBytes(parameterIndex, value);
            }
        }
    }

    /** A mapping for backward compatibility, converts a string column to a double array with a specified delimiter */
    public static class StringDoubleArrayDataTypeMapping extends DataTypeMapping<double[]> {
        StringDoubleArrayDataTypeMapping() {
            super(Types.VARCHAR, double[].class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<double[], Values> objectChunk = destChunk.asWritableObjectChunk();
            final String strValue = resultSet.getString(columnIndex);
            if (strValue == null) {
                objectChunk.set(destOffset, null);
            } else {
                objectChunk.set(destOffset, context.getArrayParser().getDoubleArray(strValue, context.isStrict()));
            }
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<double[], Values> objectChunk = srcChunk.asObjectChunk();
            final double[] value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setString(parameterIndex, context.getArrayParser().encodeArray(value));
            }
        }
    }

    /** A mapping for backward compatibility, converts a string column to a long array with a specified delimiter */
    public static class StringLongArrayDataTypeMapping extends DataTypeMapping<long[]> {
        StringLongArrayDataTypeMapping() {
            super(Types.VARCHAR, long[].class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<long[], Values> objectChunk = destChunk.asWritableObjectChunk();
            final String strValue = resultSet.getString(columnIndex);
            if (strValue == null) {
                objectChunk.set(destOffset, null);
            } else {
                objectChunk.set(destOffset, context.getArrayParser().getLongArray(strValue, context.isStrict()));
            }
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<long[], Values> objectChunk = srcChunk.asObjectChunk();
            final long[] value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setString(parameterIndex, context.getArrayParser().encodeArray(value));
            }
        }
    }

    public static class DecimalDataTypeMapping extends DataTypeMapping<BigDecimal> {
        DecimalDataTypeMapping(int sqlType) {
            super(sqlType, BigDecimal.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<BigDecimal, Values> objectChunk = destChunk.asWritableObjectChunk();
            objectChunk.set(destOffset, resultSet.getBigDecimal(columnIndex));
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<BigDecimal, Values> objectChunk = srcChunk.asObjectChunk();
            final BigDecimal value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setBigDecimal(parameterIndex, value);
            }
        }
    }

    /**
     * A mapping that converts SQL double precision floating point to Deephaven BigDecimal
     */
    public static class DoubleToDecimalDataTypeMapping extends DataTypeMapping<BigDecimal> {
        DoubleToDecimalDataTypeMapping(int sqlType) {
            super(sqlType, BigDecimal.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<BigDecimal, Values> objectChunk = destChunk.asWritableObjectChunk();
            final double value = resultSet.getDouble(columnIndex);
            if (resultSet.wasNull()) {
                objectChunk.set(destOffset, null);
            } else {
                objectChunk.set(destOffset, new BigDecimal(value));
            }
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<BigDecimal, Values> objectChunk = srcChunk.asObjectChunk();
            final BigDecimal value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setDouble(parameterIndex, value.doubleValue());
            }
        }
    }

    /**
     * A mapping that converts SQL single precision floating point to Deephaven BigDecimal
     */
    public static class FloatToDecimalDataTypeMapping extends DataTypeMapping<BigDecimal> {
        FloatToDecimalDataTypeMapping(int sqlType) {
            super(sqlType, BigDecimal.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<BigDecimal, Values> objectChunk = destChunk.asWritableObjectChunk();
            final float value = resultSet.getFloat(columnIndex);
            if (resultSet.wasNull()) {
                objectChunk.set(destOffset, null);
            } else {
                objectChunk.set(destOffset, new BigDecimal(value));
            }
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<BigDecimal, Values> objectChunk = srcChunk.asObjectChunk();
            final BigDecimal value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setFloat(parameterIndex, value.floatValue());
            }
        }
    }

    public static class IntDataTypeMapping extends DataTypeMapping<Integer> {
        IntDataTypeMapping() {
            super(Types.INTEGER, int.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableIntChunk<Values> intChunk = destChunk.asWritableIntChunk();
            final int value = resultSet.getInt(columnIndex);
            intChunk.set(destOffset, resultSet.wasNull() ? QueryConstants.NULL_INT : value);
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final IntChunk<Values> intChunk = srcChunk.asIntChunk();
            final int value = intChunk.get(srcOffset);
            if (value == QueryConstants.NULL_INT) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setInt(parameterIndex, value);
            }
        }
    }

    public static class CharDataTypeMapping extends DataTypeMapping<Character> {
        CharDataTypeMapping() {
            super(Types.CHAR, char.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableCharChunk<Values> charChunk = destChunk.asWritableCharChunk();
            final String str = resultSet.getString(columnIndex);
            // lenient for now (if a string is empty or null we map to NULL, otherwise take 1st char)
            charChunk.set(destOffset, (str == null || str.isEmpty()) ? QueryConstants.NULL_CHAR : str.charAt(0));
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final CharChunk<Values> charChunk = srcChunk.asCharChunk();
            final char value = charChunk.get(srcOffset);
            if (value == QueryConstants.NULL_CHAR) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setString(parameterIndex, String.valueOf(value));
            }
        }
    }

    public static class ShortDataTypeMapping extends DataTypeMapping<Short> {
        ShortDataTypeMapping() {
            super(Types.SMALLINT, short.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableShortChunk<Values> shortChunk = destChunk.asWritableShortChunk();
            final short value = resultSet.getShort(columnIndex);
            shortChunk.set(destOffset, resultSet.wasNull() ? QueryConstants.NULL_SHORT : value);
        }

        @Override
        public void bindFromChunk(Chunk<Values> srcChunk, int srcOffset, PreparedStatement stmt, int parameterIndex,
                Context context) throws SQLException {
            final ShortChunk<Values> shortChunk = srcChunk.asShortChunk();
            final short value = shortChunk.get(srcOffset);
            if (value == QueryConstants.NULL_SHORT) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setShort(parameterIndex, value);
            }
        }
    }

    public static class LongDataTypeMapping extends DataTypeMapping<Long> {
        LongDataTypeMapping() {
            super(Types.BIGINT, long.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableLongChunk<Values> longChunk = destChunk.asWritableLongChunk();
            final long value = resultSet.getLong(columnIndex);
            longChunk.set(destOffset, resultSet.wasNull() ? QueryConstants.NULL_LONG : value);
        }

        @Override
        public void bindFromChunk(Chunk<Values> srcChunk, int srcOffset, PreparedStatement stmt, int parameterIndex,
                Context context) throws SQLException {
            final LongChunk<Values> longChunk = srcChunk.asLongChunk();
            final long value = longChunk.get(srcOffset);
            if (value == QueryConstants.NULL_LONG) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setLong(parameterIndex, value);
            }
        }
    }

    public static class FloatDataTypeMapping extends DataTypeMapping<Float> {
        FloatDataTypeMapping(int sqlType) {
            super(sqlType, float.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableFloatChunk<Values> floatChunk = destChunk.asWritableFloatChunk();
            final float value = resultSet.getFloat(columnIndex);
            floatChunk.set(destOffset, resultSet.wasNull() ? QueryConstants.NULL_FLOAT : value);
        }

        @Override
        public void bindFromChunk(Chunk<Values> srcChunk, int srcOffset, PreparedStatement stmt, int parameterIndex,
                Context context) throws SQLException {
            final FloatChunk<Values> floatChunk = srcChunk.asFloatChunk();
            final float value = floatChunk.get(srcOffset);
            if (value == QueryConstants.NULL_FLOAT) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setFloat(parameterIndex, value);
            }
        }
    }

    /**
     * Provides a mapping from SQL decimal/numeric to Deephaven BigDecimal type.
     */
    public static class DecimalToFloatDataTypeMapping extends DataTypeMapping<Float> {
        DecimalToFloatDataTypeMapping(int sqlType) {
            super(sqlType, float.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableFloatChunk<Values> floatChunk = destChunk.asWritableFloatChunk();
            final BigDecimal value = resultSet.getBigDecimal(columnIndex);
            floatChunk.set(destOffset, value == null ? QueryConstants.NULL_FLOAT : value.floatValue());
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final FloatChunk<Values> floatChunk = srcChunk.asFloatChunk();
            final float value = floatChunk.get(srcOffset);
            if (value == QueryConstants.NULL_FLOAT) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setBigDecimal(parameterIndex, new BigDecimal(value));
            }
        }
    }

    public static class DoubleDataTypeMapping extends DataTypeMapping<Double> {
        DoubleDataTypeMapping(int sqlType) {
            super(sqlType, double.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableDoubleChunk<Values> doubleChunk = destChunk.asWritableDoubleChunk();
            final double value = resultSet.getDouble(columnIndex);
            doubleChunk.set(destOffset, resultSet.wasNull() ? QueryConstants.NULL_DOUBLE : value);
        }

        @Override
        public void bindFromChunk(Chunk<Values> srcChunk, int srcOffset, PreparedStatement stmt, int parameterIndex,
                Context context) throws SQLException {
            final DoubleChunk<Values> doubleChunk = srcChunk.asDoubleChunk();
            final double value = doubleChunk.get(srcOffset);
            if (value == QueryConstants.NULL_DOUBLE) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setDouble(parameterIndex, value);
            }
        }
    }

    /**
     * Provides a SQL decimal/numeric to double precision Deephaven type mapping
     */
    public static class DecimalToDoubleDataTypeMapping extends DataTypeMapping<Double> {
        DecimalToDoubleDataTypeMapping(int sqlType) {
            super(sqlType, double.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableDoubleChunk<Values> doubleChunk = destChunk.asWritableDoubleChunk();
            final BigDecimal value = resultSet.getBigDecimal(columnIndex);
            doubleChunk.set(destOffset, value == null ? QueryConstants.NULL_DOUBLE : value.doubleValue());
        }

        @Override
        public void bindFromChunk(Chunk<Values> srcChunk, int srcOffset, PreparedStatement stmt, int parameterIndex,
                Context context) throws SQLException {
            final DoubleChunk<Values> doubleChunk = srcChunk.asDoubleChunk();
            final double value = doubleChunk.get(srcOffset);
            if (value == QueryConstants.NULL_DOUBLE) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setBigDecimal(parameterIndex, new BigDecimal(value));
            }
        }
    }

    public static class BooleanDataTypeMapping extends DataTypeMapping<Boolean> {
        BooleanDataTypeMapping(int sqlType) {
            super(sqlType, Boolean.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<Boolean, Values> objectChunk = destChunk.asWritableObjectChunk();
            final boolean value = resultSet.getBoolean(columnIndex);
            objectChunk.set(destOffset, resultSet.wasNull() ? null : value);
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<Boolean, Values> objectChunk = srcChunk.asObjectChunk();
            final Boolean value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setBoolean(parameterIndex, value);
            }
        }
    }

    public static class DateLocalDateDataTypeMapping extends DataTypeMapping<LocalDate> {
        DateLocalDateDataTypeMapping() {
            super(Types.DATE, LocalDate.class, java.sql.Date.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<LocalDate, Values> objectChunk = destChunk.asWritableObjectChunk();

            final java.sql.Date value = resultSet.getDate(columnIndex, context.getSourceCalendar());
            if (value == null) {
                objectChunk.set(destOffset, null);
            } else {
                // simple value.toLocalDate() does not work, we need to use the specified calendar (JDBC returns DATE
                // values as midnight in the timezone of the given calendar)
                final Calendar sourceCalendar = (Calendar) context.getSourceCalendar().clone();
                sourceCalendar.setTimeInMillis(value.getTime());
                objectChunk.set(destOffset, LocalDate.of(sourceCalendar.get(Calendar.YEAR),
                        sourceCalendar.get(Calendar.MONTH) + 1,
                        sourceCalendar.get(Calendar.DAY_OF_MONTH)));
            }
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<LocalDate, Values> objectChunk = srcChunk.asObjectChunk();
            final LocalDate value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                // set date as midnight in the source calendar
                final Calendar sourceCalendar = context.getSourceCalendar();
                // noinspection MagicConstant
                sourceCalendar.set(value.getYear(), value.getMonthValue() - 1, value.getDayOfMonth(),
                        0, 0, 0);
                stmt.setDate(parameterIndex, new Date(sourceCalendar.getTimeInMillis()), sourceCalendar);
            }
        }
    }

    public static class DateInstantDataTypeMapping extends DataTypeMapping<Instant> {
        DateInstantDataTypeMapping() {
            super(Types.DATE, Instant.class, java.sql.Date.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<Instant, Values> objectChunk = destChunk.asWritableObjectChunk();
            final java.sql.Date date = resultSet.getDate(columnIndex, context.getSourceCalendar());
            objectChunk.set(destOffset, date == null
                    ? null
                    : DateTimeUtils.epochMillisToInstant(date.getTime()));
        }

        @Override
        public void bindFromChunk(Chunk<Values> srcChunk, int srcOffset, PreparedStatement stmt, int parameterIndex,
                Context context) throws SQLException {
            final ObjectChunk<Instant, Values> objectChunk = srcChunk.asObjectChunk();
            final Instant value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                stmt.setDate(parameterIndex, new Date(value.toEpochMilli()), context.getSourceCalendar());
            }
        }
    }

    public static class TimestampInstantDataTypeMapping extends DataTypeMapping<Instant> {
        TimestampInstantDataTypeMapping(int sqlType) {
            super(sqlType, Instant.class, java.sql.Timestamp.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<Instant, Values> objectChunk = destChunk.asWritableObjectChunk();
            final java.sql.Timestamp timestamp = resultSet.getTimestamp(columnIndex, context.getSourceCalendar());
            objectChunk.set(destOffset, timestamp == null
                    ? null
                    : DateTimeUtils.epochNanosToInstant(
                            timestamp.getTime() * MILLIS_TO_NANOS + timestamp.getNanos() % 1_000_000));
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<Instant, Values> objectChunk = srcChunk.asObjectChunk();
            final Instant value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                final Timestamp ts = new Timestamp(value.toEpochMilli());
                ts.setNanos((int) (DateTimeUtils.epochNanos(value) % 1_000_000_000L));
                stmt.setTimestamp(parameterIndex, ts, context.getSourceCalendar());
            }
        }
    }

    public static class DateYearDataTypeMapping extends DataTypeMapping<Integer> {
        DateYearDataTypeMapping() {
            super(Types.DATE, int.class, java.sql.Date.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableIntChunk<Values> intChunk = destChunk.asWritableIntChunk();
            final java.sql.Date yearDate = resultSet.getDate(columnIndex, context.getSourceCalendar());
            if (yearDate == null) {
                intChunk.set(destOffset, QueryConstants.NULL_INT);
            } else {
                final Calendar calendar = (Calendar) context.getSourceCalendar().clone();
                calendar.setTime(yearDate);
                intChunk.set(destOffset, calendar.get(Calendar.YEAR));
            }
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final IntChunk<Values> intChunk = srcChunk.asIntChunk();
            final int value = intChunk.get(srcOffset);
            if (value == QueryConstants.NULL_INT) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                final Calendar yearCal = (Calendar) context.getSourceCalendar().clone();
                yearCal.set(Calendar.YEAR, value);
                yearCal.set(Calendar.MONTH, Calendar.JANUARY);
                yearCal.set(Calendar.DAY_OF_MONTH, 1);
                final Date yearDate = new java.sql.Date(yearCal.getTimeInMillis());
                stmt.setDate(parameterIndex, yearDate, context.getSourceCalendar());
            }
        }
    }

    public static class TimeLocalTimeDataTypeMapping extends DataTypeMapping<LocalTime> {
        TimeLocalTimeDataTypeMapping(int sqlType) {
            super(sqlType, LocalTime.class, java.sql.Time.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableObjectChunk<LocalTime, Values> objectChunk = destChunk.asWritableObjectChunk();
            final java.sql.Time timeValue = resultSet.getTime(columnIndex, context.getSourceCalendar());
            final LocalTime localTime = timeValue == null ? null : timeValue.toLocalTime();
            final int millis = timeValue == null ? 0 : (int) (timeValue.getTime() % 1000);
            objectChunk.set(destOffset, millis == 0 ? localTime : localTime.plusNanos(millis * MILLIS_TO_NANOS));
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final ObjectChunk<LocalTime, Values> objectChunk = srcChunk.asObjectChunk();
            final LocalTime value = objectChunk.get(srcOffset);
            if (value == null) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                // note this is lossy, java.sql.Time has only milli precision
                stmt.setTime(parameterIndex, java.sql.Time.valueOf(value), context.getSourceCalendar());
            }
        }
    }

    // we map a SQL time value to a long in nanos
    // Note: this type is broken if a Calendar is used that does not match the server, as time zone conversions are not
    // symmetrical (ie you might convert a local UTC-6 time of 00:00:00 to 18:00:00 UTC for storage, which comes back
    // out as 12:00:00, since we are missing the "date" part)
    public static class TimeNanosDataTypeMapping extends DataTypeMapping<Long> {
        TimeNanosDataTypeMapping(int sqlType) {
            super(sqlType, long.class, java.sql.Time.class);
        }

        @Override
        public void bindToChunk(
                WritableChunk<Values> destChunk, int destOffset,
                ResultSet resultSet, int columnIndex, Context context) throws SQLException {
            final WritableLongChunk<Values> longChunk = destChunk.asWritableLongChunk();
            final java.sql.Time time = resultSet.getTime(columnIndex, context.getSourceCalendar());
            longChunk.set(destOffset, time == null ? QueryConstants.NULL_LONG : time.getTime() * MILLIS_TO_NANOS);
        }

        @Override
        public void bindFromChunk(
                Chunk<Values> srcChunk, int srcOffset,
                PreparedStatement stmt, int parameterIndex, Context context) throws SQLException {
            final LongChunk<Values> longChunk = srcChunk.asLongChunk();
            final long value = longChunk.get(srcOffset);
            if (value == QueryConstants.NULL_LONG) {
                stmt.setNull(parameterIndex, sqlType);
            } else {
                // note this is lossy, java.sql.Time has only milli precision
                stmt.setTime(parameterIndex, new Time(value / MILLIS_TO_NANOS), context.getSourceCalendar());
            }
        }
    }

    private static class MappingCollection {
        private Map<Class<?>, DataTypeMapping<?>> dataTypeMappingMap = new HashMap<>();
        private final Class<?> defaultDeephavenType;

        MappingCollection(Class<?> defaultDeephavenType, DataTypeMapping<?> mapping) {
            this.defaultDeephavenType = defaultDeephavenType;
            dataTypeMappingMap.put(defaultDeephavenType, mapping);
        }

        MappingCollection(Class<?> defaultDeephavenType, Map<Class<?>, DataTypeMapping<?>> dataTypeMappingMap) {
            this.defaultDeephavenType = defaultDeephavenType;
            this.dataTypeMappingMap = dataTypeMappingMap;
            if (defaultDeephavenType != null && !dataTypeMappingMap.containsKey(defaultDeephavenType)) {
                throw new IllegalArgumentException("Default Deephaven type missing from provided mappings.");
            }
        }

        static Builder builder(Class<?> defaultDeephavenType) {
            return new Builder(defaultDeephavenType);
        }

        DataTypeMapping<?> getMapping(Class<?> deephavenTpe) {
            return dataTypeMappingMap.get(deephavenTpe);
        }

        DataTypeMapping<?> getDefaultMapping() {
            return dataTypeMappingMap.get(defaultDeephavenType);
        }

        static class Builder {
            private final Map<Class<?>, DataTypeMapping<?>> dataTypeMappingMap = new HashMap<>();
            private final Class<?> defaultDeephavenType;

            private Builder(Class<?> defaultDeephavenType) {
                this.defaultDeephavenType = defaultDeephavenType;
            }

            Builder mapping(Class<?> deephavenType, DataTypeMapping<?> dataTypeMapping) {
                dataTypeMappingMap.put(deephavenType, dataTypeMapping);
                return this;
            }

            MappingCollection build() {
                return new MappingCollection(defaultDeephavenType, dataTypeMappingMap);
            }
        }
    }

    /**
     * A map of JDBC data type names to Java classes to use when interpreting schema column types from ResultSet
     * metadata. We also return the "input type" which may be necessary for logger code generation (since the type
     * mappings are not one-to-one and conversions might be necessary).
     *
     * Presently we support only standard SQL types. In the future we may want to add support for custom types for
     * different databases.
     */
    private static final Map<Integer, MappingCollection> dataTypeMappings;
    static {

        dataTypeMappings = Map.ofEntries(
                Map.entry(Types.CHAR, MappingCollection.builder(String.class)
                        .mapping(char.class, new CharDataTypeMapping())
                        .mapping(String.class, new StringDataTypeMapping(Types.CHAR)).build()),
                Map.entry(Types.NCHAR, new MappingCollection(String.class, new StringDataTypeMapping(Types.NCHAR))),
                Map.entry(Types.VARCHAR, MappingCollection.builder(String.class)
                        .mapping(String.class, new StringDataTypeMapping(Types.VARCHAR))
                        .mapping(double[].class, new StringDoubleArrayDataTypeMapping())
                        .mapping(long[].class, new StringLongArrayDataTypeMapping()).build()),
                Map.entry(Types.NVARCHAR,
                        new MappingCollection(String.class, new StringDataTypeMapping(Types.NVARCHAR))),
                Map.entry(Types.LONGVARCHAR,
                        new MappingCollection(String.class, new StringDataTypeMapping(Types.LONGVARCHAR))),
                Map.entry(Types.LONGNVARCHAR,
                        new MappingCollection(String.class, new StringDataTypeMapping(Types.LONGNVARCHAR))),
                Map.entry(Types.NUMERIC, MappingCollection.builder(BigDecimal.class)
                        .mapping(BigDecimal.class, new DecimalDataTypeMapping(Types.NUMERIC))
                        .mapping(double.class, new DecimalToDoubleDataTypeMapping(Types.NUMERIC))
                        .mapping(Double.class, new DecimalToDoubleDataTypeMapping(Types.NUMERIC))
                        .mapping(float.class, new DecimalToFloatDataTypeMapping(Types.NUMERIC))
                        .mapping(Float.class, new DecimalToFloatDataTypeMapping(Types.NUMERIC))
                        .build()),
                Map.entry(Types.DECIMAL, MappingCollection.builder(BigDecimal.class)
                        .mapping(BigDecimal.class, new DecimalDataTypeMapping(Types.DECIMAL))
                        .mapping(double.class, new DecimalToDoubleDataTypeMapping(Types.DECIMAL))
                        .mapping(Double.class, new DecimalToDoubleDataTypeMapping(Types.DECIMAL))
                        .mapping(float.class, new DecimalToFloatDataTypeMapping(Types.DECIMAL))
                        .mapping(Float.class, new DecimalToFloatDataTypeMapping(Types.DECIMAL))
                        .build()),
                Map.entry(Types.BOOLEAN, MappingCollection.builder(Boolean.class)
                        .mapping(boolean.class, new BooleanDataTypeMapping(Types.BOOLEAN))
                        .mapping(Boolean.class, new BooleanDataTypeMapping(Types.BOOLEAN)).build()),
                Map.entry(Types.BIT, MappingCollection.builder(boolean.class)
                        .mapping(boolean.class, new BooleanDataTypeMapping(Types.BIT))
                        .mapping(Boolean.class, new BooleanDataTypeMapping(Types.BIT)).build()),
                Map.entry(Types.TINYINT, MappingCollection.builder(short.class)
                        .mapping(short.class, new ShortDataTypeMapping())
                        .mapping(Short.class, new ShortDataTypeMapping()).build()),
                Map.entry(Types.SMALLINT, MappingCollection.builder(short.class)
                        .mapping(short.class, new ShortDataTypeMapping())
                        .mapping(Short.class, new ShortDataTypeMapping()).build()),
                Map.entry(Types.INTEGER, MappingCollection.builder(int.class)
                        .mapping(int.class, new IntDataTypeMapping())
                        .mapping(Integer.class, new IntDataTypeMapping()).build()),
                Map.entry(Types.BIGINT, MappingCollection.builder(long.class)
                        .mapping(long.class, new LongDataTypeMapping())
                        .mapping(Long.class, new LongDataTypeMapping()).build()),
                Map.entry(Types.REAL, MappingCollection.builder(float.class)
                        .mapping(float.class, new FloatDataTypeMapping(Types.REAL))
                        .mapping(Float.class, new FloatDataTypeMapping(Types.REAL))
                        .mapping(BigDecimal.class, new FloatToDecimalDataTypeMapping(Types.REAL)).build()),
                // counter-intuitive, but JDBC FLOAT is the same as DOUBLE (REAL is single precision float)
                Map.entry(Types.FLOAT, MappingCollection.builder(double.class)
                        .mapping(double.class, new DoubleDataTypeMapping(Types.FLOAT))
                        .mapping(Double.class, new DoubleDataTypeMapping(Types.FLOAT))
                        .mapping(BigDecimal.class, new DoubleToDecimalDataTypeMapping(Types.FLOAT)).build()),
                Map.entry(Types.DOUBLE, MappingCollection.builder(double.class)
                        .mapping(double.class, new DoubleDataTypeMapping(Types.DOUBLE))
                        .mapping(Double.class, new DoubleDataTypeMapping(Types.DOUBLE))
                        .mapping(BigDecimal.class, new DoubleToDecimalDataTypeMapping(Types.DOUBLE)).build()),
                Map.entry(Types.BINARY,
                        new MappingCollection(byte[].class, new ByteArrayDataTypeMapping(Types.BINARY))),
                Map.entry(Types.VARBINARY,
                        new MappingCollection(byte[].class, new ByteArrayDataTypeMapping(Types.VARBINARY))),
                Map.entry(Types.LONGVARBINARY,
                        new MappingCollection(byte[].class, new ByteArrayDataTypeMapping(Types.LONGVARBINARY))),
                Map.entry(Types.BLOB, new MappingCollection(byte[].class, new ByteArrayDataTypeMapping(Types.BLOB))),
                Map.entry(Types.DATE, MappingCollection.builder(LocalDate.class)
                        .mapping(LocalDate.class, new DateLocalDateDataTypeMapping())
                        .mapping(Instant.class, new DateInstantDataTypeMapping())
                        // provides the option to treat a DATE column as a String
                        .mapping(String.class, new StringDataTypeMapping(Types.DATE))
                        .build()),

                // TODO: map to LocalDateTime?
                Map.entry(Types.TIMESTAMP, MappingCollection.builder(Instant.class)
                        .mapping(Instant.class, new TimestampInstantDataTypeMapping(Types.TIMESTAMP))
                        // provides the option to treat a TIMESTAMP column as a String
                        .mapping(String.class, new StringDataTypeMapping(Types.TIMESTAMP))
                        .build()),

                // TODO: map to OffsetDateTime?
                Map.entry(Types.TIMESTAMP_WITH_TIMEZONE, MappingCollection.builder(Instant.class)
                        .mapping(Instant.class, new TimestampInstantDataTypeMapping(Types.TIMESTAMP_WITH_TIMEZONE))
                        // provides the option to treat a TIMESTAMP_WITH_TIMEZONE column as a String
                        .mapping(String.class, new StringDataTypeMapping(Types.TIMESTAMP_WITH_TIMEZONE))
                        .build()),
                Map.entry(Types.TIME, MappingCollection.builder(LocalTime.class)
                        .mapping(LocalTime.class, new TimeLocalTimeDataTypeMapping(Types.TIME))
                        .mapping(long.class, new TimeNanosDataTypeMapping(Types.TIME))
                        .mapping(Long.class, new TimeNanosDataTypeMapping(Types.TIME))
                        // provides the option to treat a TIME column as a String
                        .mapping(String.class, new StringDataTypeMapping(Types.TIME))
                        .build()),

                // TODO: map to OffsetTime?
                Map.entry(Types.TIME_WITH_TIMEZONE, MappingCollection.builder(LocalTime.class)
                        .mapping(LocalTime.class, new TimeLocalTimeDataTypeMapping(Types.TIME_WITH_TIMEZONE))
                        .mapping(long.class, new TimeNanosDataTypeMapping(Types.TIME_WITH_TIMEZONE))
                        // provides the option to treat a TIME_WITH_TIMEZONE column as a String
                        .mapping(String.class, new StringDataTypeMapping(Types.TIME_WITH_TIMEZONE))
                        .build()));
    }

    // there is a microsoft.sql.Types class, but we don't want compile-time dependencies on any particular driver
    static class SqlServerTypes {
        // matches microsoft.sql.Types.DATETIMEOFFSET value
        static final int DATETIMEOFFSET = -155;
    }

    // map custom types by Connection base class/interface, in case the driver supplies multiple Connection variants.
    private static final Map<Class<? extends Connection>, Map<Integer, MappingCollection>> driverSpecificDataTypeMappings =
            new HashMap<>();
    static {
        // here we create a map for each driver with custom types and create the appropriate mappings
        try {
            // special DATETIMEOFFSET type for SQL Server
            final Map<Integer, MappingCollection> sqlServerMap = new HashMap<>();
            sqlServerMap.put(SqlServerTypes.DATETIMEOFFSET, MappingCollection.builder(Instant.class)
                    .mapping(Instant.class, new TimestampInstantDataTypeMapping(SqlServerTypes.DATETIMEOFFSET))
                    .build());

            driverSpecificDataTypeMappings.put(
                    Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerConnection").asSubclass(Connection.class),
                    sqlServerMap);
        } catch (ClassNotFoundException ignored) {
            // ignore the driver overloads; it's not on the classpath
        }

        try {
            // YEAR MySQL type shows up as a DATE, we support mapping to an Integer
            final Map<Integer, MappingCollection> mysqlMap = new HashMap<>();
            mysqlMap.put(Types.DATE, MappingCollection.builder(null)
                    .mapping(Integer.class, new DateYearDataTypeMapping()).build());

            driverSpecificDataTypeMappings.put(
                    Class.forName("com.mysql.cj.jdbc.JdbcConnection").asSubclass(Connection.class),
                    mysqlMap);
        } catch (ClassNotFoundException ignored) {
            // ignore the driver overloads; it's not on the classpath
        }
    }

    /**
     * Get the default type mapping for converting JDBC/SQL values to/from a Deephaven column type.
     *
     * @param rs JDBC ResultSet from which to extract type information
     * @param columnIndex the index of the JDBC ResultSet column
     * @return The default mapping for the specified column
     */
    static public <T> DataTypeMapping<T> getDefaultColumnTypeMapping(final ResultSet rs, final int columnIndex) {
        return getColumnTypeMapping(rs, columnIndex, null);
    }

    /**
     * Get the default type mapping for converting JDBC/SQL values to/from a Deephaven column type.
     *
     * @param connection a JDBC Connection object, used to determine database-specific mappings
     * @param metaData a JDBC ResultSetMetaData object that will provide the SQL type information
     * @param columnIndex the index of the JDBC ResultSet column
     * @return a mapping object that can be used to map SQL/JDBC ResultSet values to and from Deephaven column values
     */
    static public <T> DataTypeMapping<T> getDefaultColumnTypeMapping(final Connection connection,
            final ResultSetMetaData metaData, final int columnIndex) {
        return getColumnTypeMapping(connection, metaData, columnIndex, null);
    }

    /**
     * Get type mapping for converting JDBC to/from Deephaven column type. If the target Deephaven type is not
     * specified, a default will be used.
     *
     * @param rs a JDBC ResultSet object that will provide the SQL type information
     * @param columnIndex the index of the JDBC ResultSet column
     * @param deephavenDataType if not null, the desired Deephaven target data type
     * @param <T> the target Deephaven type
     * @return a mapping object that can be used to map SQL/JDBC ResultSet values to and from Deephaven column values
     */
    static public <T> DataTypeMapping<T> getColumnTypeMapping(final ResultSet rs, final int columnIndex,
            Class<T> deephavenDataType) {
        try {
            return getColumnTypeMapping(rs.getStatement() == null ? null : rs.getStatement().getConnection(),
                    rs.getMetaData(), columnIndex, deephavenDataType);
        } catch (SQLException ex) {
            throw new RuntimeException("Error getting connection from ResultSet: " + ex.getMessage(), ex);
        }
    }

    /**
     * Get type mapping for converting JDBC to/from Deephaven column type. If the target Deephaven type is not
     * specified, a default will be used.
     *
     * @param connection a JDBC Connection object, used to determine database-specific mappings
     * @param rs the JDBC ResultSet
     * @param columnIndex the index of the JDBC ResultSet column
     * @param deephavenDataType if not null, the desired Deephaven target data type
     * @param <T> the target Deephaven type
     * @return a mapping object that can be used to map SQL/JDBC ResultSet values to and from Deephaven column values
     */
    static public <T> DataTypeMapping<T> getColumnTypeMapping(final Connection connection,
            final ResultSetMetaData rs, final int columnIndex, Class<T> deephavenDataType) {
        final int type;
        Class<? extends Connection> connClass = null;
        try {
            type = rs.getColumnType(columnIndex);

            // statement/connection should be null only in test cases (in which case we cannot test driver-specific
            // behavior anyway)
            if (connection != null) {
                connClass = connection.getClass();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get metadata for source column: " + columnIndex, e);
        }
        return getColumnTypeMapping(connClass, type, deephavenDataType);
    }

    /**
     * Get type mapping for converting JDBC to/from Deephaven column type. If the target Deephaven type is not
     * specified, a default will be used.
     *
     * @param connClass a JDBC Connection subclass, used to determine database-specific mappings
     * @param type the SQL type
     * @param deephavenDataType the column type, if known. If null, the default type mapping will be provided
     * @param <T> the target Deephaven type
     * @return a mapping object that can be used to map SQL/JDBC ResultSet values to and from Deephaven column values
     */
    static public <T> DataTypeMapping<T> getColumnTypeMapping(final Class<? extends Connection> connClass, int type,
            Class<? extends T> deephavenDataType) {

        // First, see if there is a driver-specific mapping that matches the specified SQL and Deephaven types.
        // If no Deephaven type is specified, and the driver specific mappings provide a default, return that.
        // Otherwise, fall back on the standard mappings.
        // Since we want to allow implementations/subclasses we have to iterate
        if (connClass != null) {
            for (Map.Entry<Class<? extends Connection>, Map<Integer, MappingCollection>> entry : driverSpecificDataTypeMappings
                    .entrySet()) {
                if (entry.getKey().isAssignableFrom(connClass)) {
                    final MappingCollection mappingCollection = entry.getValue().get(type);
                    if (mappingCollection != null) {
                        if (deephavenDataType == null && mappingCollection.getDefaultMapping() != null) {
                            // noinspection unchecked
                            return (DataTypeMapping<T>) mappingCollection.getDefaultMapping();
                        } else if (deephavenDataType != null
                                && mappingCollection.getMapping(deephavenDataType) != null) {
                            // noinspection unchecked
                            return (DataTypeMapping<T>) mappingCollection.getMapping(deephavenDataType);
                        }
                    }
                }
            }
        }

        // fall back to standard mappings
        final MappingCollection result = dataTypeMappings.get(type);

        if (result == null) {
            throw new JdbcTypeMapperException(type, null);
        }
        // noinspection unchecked
        final DataTypeMapping<T> mapping = (DataTypeMapping<T>) (deephavenDataType == null
                ? result.getDefaultMapping()
                : result.getMapping(deephavenDataType));

        if (mapping == null) {
            throw new JdbcTypeMapperException(type, deephavenDataType);
        }
        return mapping;
    }
}
