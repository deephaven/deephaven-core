//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * The data-type catalog for the pushdown fuzzer (DH-23557).
 *
 * <p>
 * Each {@code FuzzType} knows how to (a) draw values -- including the ones most likely to break a pushdown handler, (b)
 * materialize a column of them, and (c) render one as filter-expression text. Nulls are represented as Java
 * {@code null} throughout the generator, and are converted to the {@link QueryConstants} sentinel only when a primitive
 * column is materialized.
 *
 * <p>
 * The "interesting" value lists are the point of the whole bench, so they are spelled out explicitly per type rather
 * than derived. Note in particular that for every integral type the null sentinel <em>is</em> the type's minimum value,
 * so the smallest usable value is {@code MIN + 1}; for {@code char} alone the sentinel sits at the <em>top</em> of the
 * range; and for {@code float}/{@code double} the sentinel is {@code -MAX_VALUE}.
 *
 * <p>
 * Source is deliberately ASCII-only; interesting non-ASCII values are written as {@code \\uXXXX} escapes.
 */
public abstract class FuzzType {

    /** How much of a column is null. */
    public enum NullMode {
        NONE(0.0), SPARSE(0.01), HEAVY(0.4), ALL(1.0);

        public final double fraction;

        NullMode(final double fraction) {
            this.fraction = fraction;
        }
    }

    /** How widely values are spread, and whether they cluster on the dangerous ones. */
    public enum Spread {
        /** Few distinct values, so row-group statistics and dictionaries are selective. */
        NARROW,
        /** Spread across the type's range. */
        WIDE,
        /** Clustered on {@link #interesting()} -- extremes, sentinel neighbours, NaN, infinities. */
        EXTREMES
    }

    /** Somewhere to park a value that cannot be written inline in a filter expression. */
    public interface LiteralSink {
        /** Bind {@code value} to a fresh query-scope parameter and return the parameter name. */
        String bind(Object value);
    }

    private final String label;
    private final Class<?> dataType;

    FuzzType(final String label, final Class<?> dataType) {
        this.label = label;
        this.dataType = dataType;
    }

    public final String label() {
        return label;
    }

    public final Class<?> dataType() {
        return dataType;
    }

    /** Can this type back a sorted column, and therefore sorted-column pushdown? */
    public boolean sortable() {
        return true;
    }

    /**
     * Can this type be a key-value partitioning column? Constrained by the intersection of {@code PartitionFormatter}
     * (write) and {@code PartitionParser} (read).
     */
    public boolean partitionable() {
        return true;
    }

    /** Deephaven's parquet writer dictionary-encodes String columns only. */
    public boolean dictionaryFriendly() {
        return false;
    }

    /**
     * Whether parquet row-group statistics can serve this type. {@code BigDecimal}/{@code BigInteger} fall through
     * {@code MinMaxFromStatistics.getMinMaxForComparable} and must always degrade to "maybe" (DH-19666).
     */
    public boolean statisticsSupported() {
        return true;
    }

    /** Draw one non-null value. */
    protected abstract Object drawNonNull(Random random, Spread spread);

    /** The values most likely to break a handler: extremes, sentinel neighbours, NaN, infinities, boundaries. */
    public abstract List<Object> interesting();

    /** Build a column from {@code values}, where a Java {@code null} element means "null". */
    public abstract ColumnHolder<?> makeColumn(String name, List<Object> values);

    /** Render {@code value} as filter-expression text, binding through {@code sink} when it cannot be inlined. */
    public abstract String literal(Object value, LiteralSink sink);

    /** Ordering used when a column of this type must be sorted; nulls first. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Comparator<Object> comparator() {
        return Comparator.nullsFirst((a, b) -> ((Comparable) a).compareTo(b));
    }

    /** Draw one value, honouring {@code nullMode}. */
    public final Object draw(final Random random, final Spread spread, final NullMode nullMode) {
        if (nullMode == NullMode.ALL) {
            return null;
        }
        if (nullMode.fraction > 0 && random.nextDouble() < nullMode.fraction) {
            return null;
        }
        return drawNonNull(random, spread);
    }

    @Override
    public final String toString() {
        return label;
    }

    // -------------------------------------------------------------------------------------------
    // Shared helpers. These are top-level statics because Java 11 forbids static members in the
    // anonymous subclasses below.
    // -------------------------------------------------------------------------------------------

    static Object pick(final Random random, final List<Object> from) {
        return from.get(random.nextInt(from.size()));
    }

    private static List<Object> list(final Object... values) {
        return Collections.unmodifiableList(Arrays.asList(values));
    }

    private static String repeat(final String unit, final int times) {
        final StringBuilder sb = new StringBuilder(unit.length() * times);
        for (int ii = 0; ii < times; ++ii) {
            sb.append(unit);
        }
        return sb.toString();
    }

    /**
     * The epoch-nanos bounds for {@code Instant}/{@code LocalDateTime} in this bench: the whole {@code long} nanos
     * domain, less the null sentinel at {@link QueryConstants#NULL_LONG}.
     */
    private static final long MIN_EPOCH_NANOS = Long.MIN_VALUE + 1L;
    private static final long MAX_EPOCH_NANOS = Long.MAX_VALUE;

    private static Instant instantOfNanos(final long epochNanos) {
        return Instant.ofEpochSecond(Math.floorDiv(epochNanos, 1_000_000_000L),
                Math.floorMod(epochNanos, 1_000_000_000L));
    }

    // -------------------------------------------------------------------------------------------
    // Primitives
    // -------------------------------------------------------------------------------------------

    public static final FuzzType BYTE = new FuzzType("byte", byte.class) {
        private final List<Object> interesting = list(
                QueryConstants.MIN_BYTE, (byte) (QueryConstants.MIN_BYTE + 1), QueryConstants.MAX_BYTE,
                (byte) (QueryConstants.MAX_BYTE - 1), (byte) -1, (byte) 0, (byte) 1);

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return (byte) (random.nextInt(5) - 2);
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    // Skip NULL_BYTE (== Byte.MIN_VALUE) so a non-null draw is genuinely non-null.
                    return (byte) (QueryConstants.MIN_BYTE + random.nextInt(255));
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            final byte[] data = new byte[values.size()];
            for (int ii = 0; ii < data.length; ++ii) {
                final Object v = values.get(ii);
                data[ii] = v == null ? QueryConstants.NULL_BYTE : (Byte) v;
            }
            return TableTools.byteCol(name, data);
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : "(byte)" + value;
        }
    };

    public static final FuzzType SHORT = new FuzzType("short", short.class) {
        private final List<Object> interesting = list(
                QueryConstants.MIN_SHORT, (short) (QueryConstants.MIN_SHORT + 1), QueryConstants.MAX_SHORT,
                (short) (QueryConstants.MAX_SHORT - 1), (short) -1, (short) 0, (short) 1,
                (short) Byte.MIN_VALUE, (short) Byte.MAX_VALUE);

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return (short) (random.nextInt(7) - 3);
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    return (short) (QueryConstants.MIN_SHORT + 1 + random.nextInt(Short.MAX_VALUE));
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            final short[] data = new short[values.size()];
            for (int ii = 0; ii < data.length; ++ii) {
                final Object v = values.get(ii);
                data[ii] = v == null ? QueryConstants.NULL_SHORT : (Short) v;
            }
            return TableTools.shortCol(name, data);
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : "(short)" + value;
        }
    };

    public static final FuzzType INT = new FuzzType("int", int.class) {
        private final List<Object> interesting = list(
                QueryConstants.MIN_INT, QueryConstants.MIN_INT + 1, QueryConstants.MAX_INT,
                QueryConstants.MAX_INT - 1, -1, 0, 1, (int) Short.MIN_VALUE, (int) Short.MAX_VALUE);

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return random.nextInt(11) - 5;
                case EXTREMES:
                    return pick(random, interesting);
                default: {
                    final int v = random.nextInt();
                    return v == QueryConstants.NULL_INT ? 0 : v;
                }
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            final int[] data = new int[values.size()];
            for (int ii = 0; ii < data.length; ++ii) {
                final Object v = values.get(ii);
                data[ii] = v == null ? QueryConstants.NULL_INT : (Integer) v;
            }
            return TableTools.intCol(name, data);
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : String.valueOf(value);
        }
    };

    public static final FuzzType LONG = new FuzzType("long", long.class) {
        private final List<Object> interesting = list(
                QueryConstants.MIN_LONG, QueryConstants.MIN_LONG + 1, QueryConstants.MAX_LONG,
                QueryConstants.MAX_LONG - 1, -1L, 0L, 1L, (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE);

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return (long) (random.nextInt(11) - 5);
                case EXTREMES:
                    return pick(random, interesting);
                default: {
                    final long v = random.nextLong();
                    return v == QueryConstants.NULL_LONG ? 0L : v;
                }
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            final long[] data = new long[values.size()];
            for (int ii = 0; ii < data.length; ++ii) {
                final Object v = values.get(ii);
                data[ii] = v == null ? QueryConstants.NULL_LONG : (Long) v;
            }
            return TableTools.longCol(name, data);
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : value + "L";
        }
    };

    public static final FuzzType CHAR = new FuzzType("char", char.class) {
        // NULL_CHAR is Character.MAX_VALUE: for char alone the null sentinel is at the TOP of the range.
        // Lone surrogates and the UTF-8 length boundaries matter because parquet stores char as UINT_16.
        private final List<Object> interesting = list(
                QueryConstants.MIN_CHAR, (char) 1, ' ', '0', 'A', 'a', 'z',
                (char) 0x7F, (char) 0x80, (char) 0xFF, (char) 0x100, (char) 0x7FF, (char) 0x800,
                (char) 0xD800, (char) 0xDFFF, (char) 0xE000, (char) 0xFFFD,
                (char) (QueryConstants.MAX_CHAR - 1), QueryConstants.MAX_CHAR);

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return (char) ('a' + random.nextInt(5));
                case EXTREMES:
                    return pick(random, interesting);
                default: {
                    final char c = (char) random.nextInt(Character.MAX_VALUE);
                    return c == QueryConstants.NULL_CHAR ? 'x' : c;
                }
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            final char[] data = new char[values.size()];
            for (int ii = 0; ii < data.length; ++ii) {
                final Object v = values.get(ii);
                data[ii] = v == null ? QueryConstants.NULL_CHAR : (Character) v;
            }
            return TableTools.charCol(name, data);
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            // Every char goes through the query scope: escaping arbitrary chars (quotes, lone
            // surrogates, control characters) into formula text is a bug farm, and binding is exact.
            return value == null ? "null" : sink.bind(value);
        }
    };

    public static final FuzzType FLOAT = new FuzzType("float", float.class) {
        private final List<Object> interesting = list(
                QueryConstants.MIN_FINITE_FLOAT, QueryConstants.MAX_FINITE_FLOAT, QueryConstants.MIN_POS_FLOAT,
                Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NaN,
                -0.0f, 0.0f, -1.0f, 1.0f, (float) Math.PI);

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return (float) (random.nextInt(5) - 2);
                case EXTREMES:
                    return pick(random, interesting);
                default: {
                    final float v = Float.intBitsToFloat(random.nextInt());
                    return v == QueryConstants.NULL_FLOAT ? 0.0f : v;
                }
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            final float[] data = new float[values.size()];
            for (int ii = 0; ii < data.length; ++ii) {
                final Object v = values.get(ii);
                data[ii] = v == null ? QueryConstants.NULL_FLOAT : (Float) v;
            }
            return TableTools.floatCol(name, data);
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            if (value == null) {
                return "null";
            }
            final float f = (Float) value;
            return Float.isNaN(f) || Float.isInfinite(f) ? sink.bind(value) : f + "f";
        }

        @Override
        public Comparator<Object> comparator() {
            return Comparator.nullsFirst(Comparator.comparingDouble(o -> (Float) o));
        }
    };

    public static final FuzzType DOUBLE = new FuzzType("double", double.class) {
        private final List<Object> interesting = list(
                QueryConstants.MIN_FINITE_DOUBLE, QueryConstants.MAX_FINITE_DOUBLE, QueryConstants.MIN_POS_DOUBLE,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN,
                -0.0d, 0.0d, -1.0d, 1.0d, Math.PI);

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return (double) (random.nextInt(5) - 2);
                case EXTREMES:
                    return pick(random, interesting);
                default: {
                    final double v = Double.longBitsToDouble(random.nextLong());
                    return v == QueryConstants.NULL_DOUBLE ? 0.0d : v;
                }
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            final double[] data = new double[values.size()];
            for (int ii = 0; ii < data.length; ++ii) {
                final Object v = values.get(ii);
                data[ii] = v == null ? QueryConstants.NULL_DOUBLE : (Double) v;
            }
            return TableTools.doubleCol(name, data);
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            if (value == null) {
                return "null";
            }
            final double d = (Double) value;
            return Double.isNaN(d) || Double.isInfinite(d) ? sink.bind(value) : String.valueOf(d);
        }

        @Override
        public Comparator<Object> comparator() {
            return Comparator.nullsFirst(Comparator.comparingDouble(o -> (Double) o));
        }
    };

    public static final FuzzType BOOLEAN = new FuzzType("Boolean", Boolean.class) {
        private final List<Object> interesting = list(Boolean.TRUE, Boolean.FALSE);

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            return random.nextBoolean();
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            return TableTools.booleanCol(name, values.toArray(new Boolean[0]));
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : String.valueOf(value);
        }
    };

    // -------------------------------------------------------------------------------------------
    // Objects
    // -------------------------------------------------------------------------------------------

    public static final FuzzType STRING = new FuzzType("String", String.class) {
        // Parquet BINARY min/max are computed by unsigned byte-wise UTF-8 comparison, while
        // ComparablePushdownHandler compares with String.compareTo (UTF-16 code-unit order). The two
        // orders disagree once a supplementary-plane character meets a high-BMP one, so both
        // U+E000 (3-byte UTF-8, low UTF-16) and U+1F600 (4-byte UTF-8, high UTF-16) are present.
        private final List<Object> interesting = list(
                "", " ", "\0", "0", "A", "a", "AA", "Aa", "aA", "z", "zz",
                "val", "val2",
                "\u00FF", "\u0100", "\u07FF", "\u0800", "\uE000", "\uFFFD",
                "\uD83D\uDE00", "\uD800\uDC00",
                "\u00E9", "e\u0301",
                repeat("long", 5000));

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return "s" + random.nextInt(4);
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    return Long.toString(random.nextLong(), 36);
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            return TableTools.stringCol(name, values.toArray(new String[0]));
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            if (value == null) {
                return "null";
            }
            final String s = (String) value;
            // Backtick literals cannot safely express a backtick, a backslash, or control characters.
            for (int ii = 0; ii < s.length(); ++ii) {
                final char c = s.charAt(ii);
                if (c == '`' || c == '\\' || c < ' ' || Character.isSurrogate(c)) {
                    return sink.bind(s);
                }
            }
            return "`" + s + "`";
        }

        @Override
        public boolean dictionaryFriendly() {
            return true;
        }
    };

    public static final FuzzType INSTANT = new FuzzType("Instant", Instant.class) {
        // Instants are stored as epoch nanos in a long, so the range is bounded by the long sentinel:
        // NULL_LONG nanos is null and MIN_LONG + 1 nanos is the true minimum (~1677-09-21).
        private final List<Object> interesting = list(
                instantOfNanos(0L),
                instantOfNanos(-1L),
                instantOfNanos(1L),
                instantOfNanos(MIN_EPOCH_NANOS),
                instantOfNanos(MIN_EPOCH_NANOS + 1),
                instantOfNanos(MAX_EPOCH_NANOS),
                instantOfNanos(MAX_EPOCH_NANOS - 1),
                instantOfNanos(1_000_000_000L));

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return instantOfNanos(1_700_000_000_000_000_000L + random.nextInt(5) * 1_000_000_000L);
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    // Clamp rather than scale: SAFE_MAX - SAFE_MIN overflows a long, so any
                    // range-mapping arithmetic here wraps and produces unrepresentable instants.
                    return instantOfNanos(Math.max(MIN_EPOCH_NANOS,
                            Math.min(MAX_EPOCH_NANOS, random.nextLong())));
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            return TableTools.instantCol(name, values.toArray(new Instant[0]));
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            // Bind rather than format: the DateTimeUtils literal syntax cannot express these extremes.
            return value == null ? "null" : sink.bind(value);
        }
    };

    public static final FuzzType LOCAL_DATE = new FuzzType("LocalDate", LocalDate.class) {
        // The writer stores LocalDate through DateTimeUtils.epochDaysAsInt -> Math.toIntExact, so
        // LocalDate.MIN/MAX cannot be written; the reachable extremes are the int epoch-day bounds.
        private final List<Object> interesting = list(
                LocalDate.ofEpochDay(0L),
                LocalDate.ofEpochDay(-1L),
                LocalDate.ofEpochDay(QueryConstants.MIN_INT + 1L),
                LocalDate.ofEpochDay(QueryConstants.MAX_INT),
                LocalDate.parse("2024-02-29"),
                LocalDate.parse("1900-03-01"),
                LocalDate.parse("2000-01-01"));

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return LocalDate.ofEpochDay(19_000L + random.nextInt(5));
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    return LocalDate.ofEpochDay(random.nextInt(200_000) - 100_000);
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            return TableTools.col(name, values.toArray(new LocalDate[0]));
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : sink.bind(value);
        }
    };

    public static final FuzzType LOCAL_TIME = new FuzzType("LocalTime", LocalTime.class) {
        private final List<Object> interesting = list(
                LocalTime.MIDNIGHT, LocalTime.NOON, LocalTime.MIN, LocalTime.MAX,
                LocalTime.ofNanoOfDay(1L),
                LocalTime.ofNanoOfDay(999_999L),
                LocalTime.parse("12:34:56.789012345"));

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return LocalTime.ofSecondOfDay(random.nextInt(5));
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    return LocalTime.ofNanoOfDay(Math.floorMod(random.nextLong(), 86_400_000_000_000L));
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            return TableTools.col(name, values.toArray(new LocalTime[0]));
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : sink.bind(value);
        }
    };

    public static final FuzzType LOCAL_DATE_TIME = new FuzzType("LocalDateTime", LocalDateTime.class) {
        private final List<Object> interesting = list(
                LocalDateTime.parse("1970-01-01T00:00:00"),
                LocalDateTime.parse("1969-12-31T23:59:59.999999999"),
                LocalDateTime.parse("1969-12-31T23:59:59"),
                LocalDateTime.parse("1900-06-15T12:30:00.123456789"),
                LocalDateTime.parse("2024-02-29T12:00:00"),
                LocalDateTime.parse("2261-12-31T23:59:59.999999999"),
                LocalDateTime.parse("1678-01-01T00:00:00"),
                LocalDateTime.parse("2000-01-01T00:00:00.000000001"));

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return LocalDateTime.parse("2024-01-01T00:00:0" + random.nextInt(5));
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    return LocalDateTime.ofEpochSecond(random.nextInt(2_000_000_000),
                            random.nextInt(1_000_000_000), ZoneOffset.UTC);
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            return TableTools.col(name, values.toArray(new LocalDateTime[0]));
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : sink.bind(value);
        }

        @Override
        public boolean partitionable() {
            // PartitionFormatter/PartitionParser have no LocalDateTime entry.
            return false;
        }
    };

    public static final FuzzType BIG_DECIMAL = new FuzzType("BigDecimal", BigDecimal.class) {
        // Precision and scale are computed from the data at write time, so the extremes here are scale
        // extremes. Note that equals() is scale-sensitive while compareTo() is not.
        private final List<Object> interesting = list(
                BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ONE.negate(),
                new BigDecimal("1.0"), new BigDecimal("1.00"),
                new BigDecimal("0.000000000000000000001"),
                new BigDecimal("-0.000000000000000000001"),
                new BigDecimal(BigInteger.TEN.pow(30)),
                new BigDecimal(BigInteger.TEN.pow(30).negate()));

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return BigDecimal.valueOf(random.nextInt(5));
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    return BigDecimal.valueOf(random.nextInt(1_000_000), random.nextInt(6));
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            return TableTools.col(name, values.toArray(new BigDecimal[0]));
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : sink.bind(value);
        }

        @Override
        public boolean statisticsSupported() {
            return false;
        }

        @Override
        public boolean sortable() {
            // Written with a computed precision/scale; SortedColumnsAttribute round-tripping for
            // BigDecimal is not what this bench is probing.
            return false;
        }
    };

    public static final FuzzType BIG_INTEGER = new FuzzType("BigInteger", BigInteger.class) {
        private final List<Object> interesting = list(
                BigInteger.ZERO, BigInteger.ONE, BigInteger.ONE.negate(), BigInteger.TEN,
                BigInteger.valueOf(Long.MAX_VALUE), BigInteger.valueOf(Long.MIN_VALUE),
                BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
                BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
                BigInteger.ONE.shiftLeft(200), BigInteger.ONE.shiftLeft(200).negate());

        @Override
        protected Object drawNonNull(final Random random, final Spread spread) {
            switch (spread) {
                case NARROW:
                    return BigInteger.valueOf(random.nextInt(5));
                case EXTREMES:
                    return pick(random, interesting);
                default:
                    return BigInteger.valueOf(random.nextLong());
            }
        }

        @Override
        public List<Object> interesting() {
            return interesting;
        }

        @Override
        public ColumnHolder<?> makeColumn(final String name, final List<Object> values) {
            return TableTools.col(name, values.toArray(new BigInteger[0]));
        }

        @Override
        public String literal(final Object value, final LiteralSink sink) {
            return value == null ? "null" : sink.bind(value);
        }

        @Override
        public boolean statisticsSupported() {
            return false;
        }
    };

    /** Every type the bench knows about. */
    public static final List<FuzzType> ALL = Collections.unmodifiableList(Arrays.asList(
            BYTE, SHORT, INT, LONG, CHAR, FLOAT, DOUBLE, BOOLEAN,
            STRING, INSTANT, LOCAL_DATE, LOCAL_TIME, LOCAL_DATE_TIME, BIG_DECIMAL, BIG_INTEGER));

    /** Draw a type uniformly at random -- type choice is pure random, by design. */
    public static FuzzType random(final Random random) {
        return ALL.get(random.nextInt(ALL.size()));
    }
}
