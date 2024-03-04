package io.deephaven.parquet.table;

import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * This class takes an object read from a parquet file and formats it to a string, only for the supported types.
 */
enum PartitionFormatter {
    ForString {
        @Override
        public String format(@NotNull final Object obj) {
            return (String) obj;
        }
    },
    ForBoolean {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Boolean) obj).toString();
        }
    },
    ForChar {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Character) obj).toString();
        }
    },
    ForByte {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Byte) obj).toString();
        }
    },
    ForShort {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Short) obj).toString();
        }
    },
    ForInt {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Integer) obj).toString();
        }
    },
    ForLong {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Long) obj).toString();
        }
    },
    ForFloat {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Float) obj).toString();
        }
    },
    ForDouble {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Double) obj).toString();
        }
    },
    ForBigInteger {
        @Override
        public String format(@NotNull final Object obj) {
            return ((BigInteger) obj).toString();
        }
    },
    ForBigDecimal {
        @Override
        public String format(@NotNull final Object obj) {
            return ((BigDecimal) obj).toString();
        }
    },
    ForInstant {
        @Override
        public String format(@NotNull final Object obj) {
            return ((Instant) obj).toString();
        }
    },
    ForLocalDate {
        @Override
        public String format(@NotNull final Object obj) {
            return DateTimeUtils.formatDate((LocalDate) obj);
        }
    },
    ForLocalTime {
        @Override
        public String format(@NotNull final Object obj) {
            return ((LocalTime) obj).toString();
        }
    };

    private static final Map<Class<?>, PartitionFormatter> typeMap = new HashMap<>();
    static {
        typeMap.put(String.class, ForString);
        typeMap.put(Boolean.class, ForBoolean);
        typeMap.put(Character.class, ForChar);
        typeMap.put(Byte.class, ForByte);
        typeMap.put(Short.class, ForShort);
        typeMap.put(Integer.class, ForInt);
        typeMap.put(Long.class, ForLong);
        typeMap.put(Float.class, ForFloat);
        typeMap.put(Double.class, ForDouble);
        typeMap.put(BigInteger.class, ForBigInteger);
        typeMap.put(BigDecimal.class, ForBigDecimal);
        typeMap.put(Instant.class, ForInstant);
        typeMap.put(LocalDate.class, ForLocalDate);
        typeMap.put(LocalTime.class, ForLocalTime);
    }

    abstract String format(@NotNull Object obj);

    static String formatToString(final Object obj) {
        if (obj == null) {
            return "null";
        }
        final PartitionFormatter formatter = typeMap.get(obj.getClass());
        if (formatter != null) {
            return formatter.format(obj);
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + obj.getClass().getSimpleName());
        }
    }
}
