//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.configuration.Configuration;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.function.LongFunction;

interface PageMaterializer {

    PageMaterializerFactory ByteFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new ByteMaterializer(dataReader, (byte) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new ByteMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory ShortFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new ShortMaterializer(dataReader, (short) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new ShortMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory IntFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new IntMaterializer(dataReader, (int) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new IntMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory CharFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new CharMaterializer(dataReader, (char) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new CharMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory LocalDateFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalDateMaterializer(dataReader, (LocalDate) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalDateMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory LongFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LongMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LongMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory TimestampNanosFromMillisFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new TimestampNanosFromMillisMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new TimestampNanosFromMillisMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory TimestampNanosFromMicrosFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new TimestampNanosFromMicrosMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new TimestampNanosFromMicrosMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory LongFromUnsignedIntFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LongFromUnsignedIntMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LongFromUnsignedIntMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory LocalTimeFromMillisFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromMillisMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromMillisMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory LocalTimeFromMicrosFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromMicrosMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromMicrosMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory LocalTimeFromNanosFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromNanosMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromNanosMaterializer(dataReader, numValues);
        }
    };

    class LocalDateTimeFactory {
        static PageMaterializerFactory create(final LongFunction<LocalDateTime> converter) {
            return new PageMaterializerFactory() {
                @Override
                public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue,
                        int numValues) {
                    return new LocalDateTimeMaterializer(dataReader, (LocalDateTime) nullValue, numValues, converter);
                }

                @Override
                public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
                    return new LocalDateTimeMaterializer(dataReader, numValues, converter);
                }
            };
        }
    }

    PageMaterializerFactory LocalDateTimeFromMillisFactory =
            LocalDateTimeFactory.create(ParquetTimeUtils::epochMillisToLocalDateTimeUTC);
    PageMaterializerFactory LocalDateTimeFromMicrosFactory =
            LocalDateTimeFactory.create(ParquetTimeUtils::epochMicrosToLocalDateTimeUTC);
    PageMaterializerFactory LocalDateTimeFromNanosFactory =
            LocalDateTimeFactory.create(ParquetTimeUtils::epochNanosToLocalDateTimeUTC);

    PageMaterializerFactory InstantFromInt96Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new InstantFromInt96Materializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new InstantFromInt96Materializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory FloatFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new FloatMaterializer(dataReader, (float) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new FloatMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory DoubleFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new DoubleMaterializer(dataReader, (double) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new DoubleMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory BoolFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BoolMaterializer(dataReader, (byte) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BoolMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory StringFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new StringMaterializer(dataReader, (String) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new StringMaterializer(dataReader, numValues);
        }
    };

    PageMaterializerFactory BlobFactory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BlobMaterializer(dataReader, (Binary) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BlobMaterializer(dataReader, numValues);
        }
    };

    static PageMaterializerFactory factoryForType(@NotNull final PrimitiveType primitiveType) {
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();
        final LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
        switch (primitiveTypeName) {
            case INT32:
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType =
                            (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalTypeAnnotation;
                    if (intLogicalType.isSigned()) {
                        switch (intLogicalType.getBitWidth()) {
                            case 8:
                                return ByteFactory;
                            case 16:
                                return ShortFactory;
                            case 32:
                                return IntFactory;
                        }
                    } else {
                        switch (intLogicalType.getBitWidth()) {
                            case 8:
                            case 16:
                                return CharFactory;
                            case 32:
                                return LongFromUnsignedIntFactory;
                        }
                    }
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    return LocalDateFactory;
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType =
                            (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalTypeAnnotation;
                    if (timeLogicalType.getUnit() != LogicalTypeAnnotation.TimeUnit.MILLIS) {
                        throw new IllegalArgumentException(
                                "Expected unit type to be MILLIS, found " + timeLogicalType.getUnit());
                    }
                    // isAdjustedToUTC parameter is ignored while reading LocalTime from Parquet files
                    return LocalTimeFromMillisFactory;
                }
                return IntFactory;
            case INT64:
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType =
                            (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation;
                    if (timestampLogicalType.isAdjustedToUTC()) {
                        // The column will store nanoseconds elapsed since epoch as long values
                        switch (timestampLogicalType.getUnit()) {
                            case MILLIS:
                                return TimestampNanosFromMillisFactory;
                            case MICROS:
                                return TimestampNanosFromMicrosFactory;
                            case NANOS:
                                return LongFactory;
                        }
                    } else {
                        // The column will be stored as LocalDateTime values
                        // Ref:https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#local-semantics-timestamps-not-normalized-to-utc
                        switch (timestampLogicalType.getUnit()) {
                            case MILLIS:
                                return LocalDateTimeFromMillisFactory;
                            case MICROS:
                                return LocalDateTimeFromMicrosFactory;
                            case NANOS:
                                return LocalDateTimeFromNanosFactory;
                        }
                    }
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType =
                            (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalTypeAnnotation;
                    // isAdjustedToUTC parameter is ignored while reading LocalTime from Parquet files
                    switch (timeLogicalType.getUnit()) {
                        case MICROS:
                            return LocalTimeFromMicrosFactory;
                        case NANOS:
                            return LocalTimeFromNanosFactory;
                        default:
                            throw new IllegalArgumentException("Unsupported unit=" + timeLogicalType.getUnit());
                    }
                }
                return LongFactory;
            case INT96:
                return InstantFromInt96Factory;
            case FLOAT:
                return FloatFactory;
            case DOUBLE:
                return DoubleFactory;
            case BOOLEAN:
                return BoolFactory;
            case BINARY:
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                    return StringFactory;
                }
            case FIXED_LEN_BYTE_ARRAY:
                return BlobFactory;
            default:
                throw new RuntimeException("Unexpected type name:" + primitiveTypeName);
        }
    }

    void fillNulls(int startIndex, int endIndex);

    void fillValues(int startIndex, int endIndex);

    Object fillAll();

    Object data();

    class ByteMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final byte nullValue;
        final byte[] data;

        ByteMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, (byte) 0, numValues);
        }

        ByteMaterializer(ValuesReader dataReader, byte nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new byte[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = (byte) dataReader.readInteger();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class ShortMaterializer implements PageMaterializer {
        final ValuesReader dataReader;

        final short nullValue;
        final short[] data;

        ShortMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, (short) 0, numValues);
        }

        ShortMaterializer(ValuesReader dataReader, short nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new short[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = (short) dataReader.readInteger();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class IntMaterializer implements PageMaterializer {
        final ValuesReader dataReader;

        final int nullValue;
        final int[] data;

        IntMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        IntMaterializer(ValuesReader dataReader, int nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new int[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readInteger();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class CharMaterializer implements PageMaterializer {
        final ValuesReader dataReader;

        final char nullValue;
        final char[] data;

        CharMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, (char) 0, numValues);
        }

        CharMaterializer(ValuesReader dataReader, char nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new char[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = (char) dataReader.readInteger();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class LocalDateMaterializer implements PageMaterializer {
        final ValuesReader dataReader;

        final LocalDate nullValue;
        final LocalDate[] data;

        LocalDateMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        LocalDateMaterializer(ValuesReader dataReader, LocalDate nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new LocalDate[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = DateTimeUtils.epochDaysAsIntToLocalDate(dataReader.readInteger());
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    abstract class LongMaterializerBase implements PageMaterializer {

        final long nullValue;
        final long[] data;

        /**
         * @return a long value read from the dataReader.
         */
        abstract long readLong();

        LongMaterializerBase(long nullValue, int numValues) {
            this.nullValue = nullValue;
            this.data = new long[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = readLong();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class LongMaterializer extends LongMaterializerBase implements PageMaterializer {

        final ValuesReader dataReader;

        LongMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        LongMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        long readLong() {
            return dataReader.readLong();
        }
    }

    class TimestampNanosFromMillisMaterializer extends LongMaterializerBase implements PageMaterializer {

        final ValuesReader dataReader;

        TimestampNanosFromMillisMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        TimestampNanosFromMillisMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        long readLong() {
            return DateTimeUtils.millisToNanos(dataReader.readLong());
        }
    }

    class TimestampNanosFromMicrosMaterializer extends LongMaterializerBase implements PageMaterializer {

        final ValuesReader dataReader;

        TimestampNanosFromMicrosMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        TimestampNanosFromMicrosMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        long readLong() {
            return DateTimeUtils.microsToNanos(dataReader.readLong());
        }
    }

    class LongFromUnsignedIntMaterializer extends LongMaterializerBase implements PageMaterializer {

        final ValuesReader dataReader;

        LongFromUnsignedIntMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        LongFromUnsignedIntMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        long readLong() {
            return Integer.toUnsignedLong(dataReader.readInteger());
        }
    }

    abstract class LocalTimeMaterializerBase implements PageMaterializer {

        final LocalTime nullValue;
        final LocalTime[] data;

        /**
         * @return a {@link LocalTime} value read from the dataReader.
         */
        abstract LocalTime readNext();

        LocalTimeMaterializerBase(LocalTime nullValue, int numValues) {
            this.nullValue = nullValue;
            this.data = new LocalTime[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = readNext();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class LocalTimeFromMillisMaterializer extends LocalTimeMaterializerBase implements PageMaterializer {

        final ValuesReader dataReader;

        LocalTimeFromMillisMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        LocalTimeFromMillisMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        LocalTime readNext() {
            return DateTimeUtils.millisOfDayToLocalTime(dataReader.readInteger());
        }
    }

    class LocalTimeFromMicrosMaterializer extends LocalTimeMaterializerBase implements PageMaterializer {

        final ValuesReader dataReader;

        LocalTimeFromMicrosMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        LocalTimeFromMicrosMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        LocalTime readNext() {
            return DateTimeUtils.microsOfDayToLocalTime(dataReader.readLong());
        }
    }

    class LocalTimeFromNanosMaterializer extends LocalTimeMaterializerBase implements PageMaterializer {

        final ValuesReader dataReader;

        LocalTimeFromNanosMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        LocalTimeFromNanosMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        LocalTime readNext() {
            return DateTimeUtils.nanosOfDayToLocalTime(dataReader.readLong());
        }
    }

    class LocalDateTimeMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final LocalDateTime nullValue;
        final LocalDateTime[] data;
        final LongFunction<LocalDateTime> converter;

        LocalDateTimeMaterializer(ValuesReader dataReader, int numValues, LongFunction<LocalDateTime> converter) {
            this(dataReader, null, numValues, converter);
        }

        LocalDateTimeMaterializer(ValuesReader dataReader, LocalDateTime nullValue, int numValues,
                LongFunction<LocalDateTime> converter) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new LocalDateTime[numValues];
            this.converter = converter;
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = converter.apply(dataReader.readLong());
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    /**
     * {@link PageMaterializer} implementation for {@link Instant}s stored as Int96s representing an Impala format
     * Timestamp (nanoseconds of day and Julian date encoded as 8 bytes and 4 bytes, respectively)
     */
    class InstantFromInt96Materializer extends LongMaterializerBase implements PageMaterializer {

        /*
         * Potential references/points of comparison for this algorithm:
         * https://github.com/apache/iceberg/pull/1184/files
         * https://github.com/apache/arrow/blob/master/cpp/src/parquet/types.h (last retrieved as
         * https://github.com/apache/arrow/blob/d5a2aa2ffb1c2fc4f3ca48c829fcdba80ec67916/cpp/src/parquet/types.h)
         */
        private static final long NANOS_PER_DAY = 86400L * 1000 * 1000 * 1000;
        private static final int JULIAN_OFFSET_TO_UNIX_EPOCH_DAYS = 2_440_588;
        private static long offset;
        static {
            final String referenceTimeZone =
                    Configuration.getInstance().getStringWithDefault("deephaven.parquet.referenceTimeZone", "UTC");
            setReferenceTimeZone(referenceTimeZone);
        }

        final ValuesReader dataReader;

        InstantFromInt96Materializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        InstantFromInt96Materializer(ValuesReader dataReader, long nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        /**
         * Allows overriding the time zone to be used when interpreting Int96 timestamp values. Default is UTC. Can be
         * set globally with the parameter deephaven.parquet.referenceTimeZone. Valid values are time zone strings that
         * would be used in {@link DateTimeUtils#parseInstant(String) parseInstant}, such as NY.
         */
        private static void setReferenceTimeZone(@NotNull final String timeZone) {
            offset = DateTimeUtils.nanosOfDay(DateTimeUtils.parseInstant("1970-01-01T00:00:00 " + timeZone),
                    ZoneId.of("UTC"));
        }

        @Override
        long readLong() {
            final ByteBuffer resultBuffer = ByteBuffer.wrap(dataReader.readBytes().getBytesUnsafe());
            resultBuffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
            final long nanos = resultBuffer.getLong();
            final int julianDate = resultBuffer.getInt();
            return (julianDate - JULIAN_OFFSET_TO_UNIX_EPOCH_DAYS) * (NANOS_PER_DAY) + nanos + offset;
        }
    }

    class FloatMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final float nullValue;
        final float[] data;

        FloatMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        FloatMaterializer(ValuesReader dataReader, float nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new float[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readFloat();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class DoubleMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final double nullValue;
        final double[] data;

        DoubleMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        DoubleMaterializer(ValuesReader dataReader, double nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new double[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readDouble();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class BoolMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final byte nullValue;
        final byte[] data;

        private BoolMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, (byte) 0, numValues);
        }

        private BoolMaterializer(ValuesReader dataReader, byte nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new byte[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = (byte) (dataReader.readBoolean() ? 1 : 0);
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class StringMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final String nullValue;
        final String[] data;

        StringMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        StringMaterializer(ValuesReader dataReader, String nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new String[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readBytes().toStringUsingUTF8();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class BlobMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final Binary nullValue;
        final Binary[] data;

        BlobMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        BlobMaterializer(ValuesReader dataReader, Binary nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new Binary[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readBytes();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }
}
