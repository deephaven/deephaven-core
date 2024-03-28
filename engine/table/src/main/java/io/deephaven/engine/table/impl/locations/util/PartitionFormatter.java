//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.util;

import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * This class takes a partition value object and formats it to a {@link String}. Useful when generating partitioning
 * paths for table. Complementary to {@link PartitionParser}.
 */
public enum PartitionFormatter {
    ForString {
        @Override
        public String formatObject(@NotNull final Object value) {
            return (String) value;
        }
    },
    ForBoolean {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Boolean) value).toString();
        }
    },
    ForChar {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Character) value).toString();
        }
    },
    ForByte {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Byte) value).toString();
        }
    },
    ForShort {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Short) value).toString();
        }
    },
    ForInt {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Integer) value).toString();
        }
    },
    ForLong {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Long) value).toString();
        }
    },
    ForFloat {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Float) value).toString();
        }
    },
    ForDouble {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Double) value).toString();
        }
    },
    ForBigInteger {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((BigInteger) value).toString();
        }
    },
    ForBigDecimal {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((BigDecimal) value).toString();
        }
    },
    ForInstant {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((Instant) value).toString();
        }
    },
    ForLocalDate {
        @Override
        public String formatObject(@NotNull final Object value) {
            return DateTimeUtils.formatDate((LocalDate) value);
        }
    },
    ForLocalTime {
        @Override
        public String formatObject(@NotNull final Object value) {
            return ((LocalTime) value).toString();
        }
    };

    private static final Map<Class<?>, PartitionFormatter> typeMap = new HashMap<>();
    static {
        typeMap.put(String.class, ForString);
        typeMap.put(Boolean.class, ForBoolean);
        typeMap.put(boolean.class, ForBoolean);
        typeMap.put(Character.class, ForChar);
        typeMap.put(char.class, ForChar);
        typeMap.put(Byte.class, ForByte);
        typeMap.put(byte.class, ForByte);
        typeMap.put(Short.class, ForShort);
        typeMap.put(short.class, ForShort);
        typeMap.put(Integer.class, ForInt);
        typeMap.put(int.class, ForInt);
        typeMap.put(Long.class, ForLong);
        typeMap.put(long.class, ForLong);
        typeMap.put(Float.class, ForFloat);
        typeMap.put(float.class, ForFloat);
        typeMap.put(Double.class, ForDouble);
        typeMap.put(double.class, ForDouble);
        typeMap.put(BigInteger.class, ForBigInteger);
        typeMap.put(BigDecimal.class, ForBigDecimal);
        typeMap.put(Instant.class, ForInstant);
        typeMap.put(LocalDate.class, ForLocalDate);
        typeMap.put(LocalTime.class, ForLocalTime);
    }

    abstract String formatObject(@NotNull final Object value);

    /**
     * Takes a partition value object and returns a formatted string. Returns an empty string if the object is null.
     */
    public String format(@Nullable final Object value) {
        if (value == null) {
            return "";
        }
        return formatObject(value);
    }

    /**
     * Takes a partitioning column type and returns the corresponding formatter.
     */
    public static PartitionFormatter getFormatterForType(@NotNull final Class<?> clazz) {
        final PartitionFormatter formatter = typeMap.get(clazz);
        if (formatter != null) {
            return formatter;
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + clazz.getSimpleName());
        }
    }
}
