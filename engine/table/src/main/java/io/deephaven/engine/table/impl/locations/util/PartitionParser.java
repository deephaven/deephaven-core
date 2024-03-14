//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.util;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Objects;

import static io.deephaven.util.type.TypeUtils.box;

/**
 * Re-usable parsers for partition values encountered as {@link String strings}, e.g. when inferring/assigning partition
 * values from elements in a path.
 */
public enum PartitionParser {

    ForString(String.class) {
        @Override
        String parseToType(@NotNull final String stringValue) {
            return stringValue;
        }
    },
    ForBoolean(Boolean.class) {
        @Override
        Boolean parseToType(@NotNull final String stringValue) {
            return Boolean.parseBoolean(stringValue);
        }
    },
    ForChar(Character.class) {
        @Override
        Character parseToType(@NotNull final String stringValue) {
            if (stringValue.length() != 1) {
                throw new IllegalArgumentException(String.format(
                        "Invalid value length %d, expected length 1", stringValue.length()));
            }
            return stringValue.charAt(0);
        }
    },
    ForByte(Byte.class) {
        @Override
        Byte parseToType(@NotNull final String stringValue) {
            return box(Byte.parseByte(stringValue));
        }
    },
    ForShort(Short.class) {
        @Override
        Short parseToType(@NotNull final String stringValue) {
            return box(Short.parseShort(stringValue));
        }
    },
    ForInt(Integer.class) {
        @Override
        Integer parseToType(@NotNull final String stringValue) {
            return box(Integer.parseInt(stringValue));
        }
    },
    ForLong(Long.class) {
        @Override
        Long parseToType(@NotNull final String stringValue) {
            return box(Long.parseLong(stringValue));
        }
    },
    ForFloat(Float.class) {
        @Override
        Float parseToType(@NotNull final String stringValue) {
            return box(Float.parseFloat(stringValue));
        }
    },
    ForDouble(Double.class) {
        @Override
        Double parseToType(@NotNull final String stringValue) {
            return box(Double.parseDouble(stringValue));
        }
    },
    ForBigInteger(BigInteger.class) {
        @Override
        BigInteger parseToType(@NotNull final String stringValue) {
            return new BigInteger(stringValue);
        }
    },
    ForBigDecimal(BigDecimal.class) {
        @Override
        BigDecimal parseToType(@NotNull final String stringValue) {
            return new BigDecimal(stringValue);
        }
    },
    ForInstant(Instant.class) {
        @Override
        Instant parseToType(@NotNull final String stringValue) {
            return DateTimeUtils.parseInstant(stringValue);
        }
    },
    ForLocalDate(LocalDate.class) {
        @Override
        LocalDate parseToType(@NotNull final String stringValue) {
            return DateTimeUtils.parseLocalDate(stringValue);
        }
    },
    ForLocalTime(LocalTime.class) {
        @Override
        LocalTime parseToType(@NotNull final String stringValue) {
            return DateTimeUtils.parseLocalTime(stringValue);
        }
    },
    ForZonedDateTime(ZonedDateTime.class) {
        @Override
        ZonedDateTime parseToType(@NotNull final String stringValue) {
            return DateTimeUtils.parseZonedDateTime(stringValue);
        }
    };

    private final Class<? extends Comparable<?>> resultType;

    /**
     * Construct a PartitionParser for the supplied types.
     *
     * @param resultType The result type for {@link #parse(String)}
     */
    PartitionParser(@NotNull final Class<? extends Comparable<?>> resultType) {
        this.resultType = Objects.requireNonNull(resultType);
    }

    /**
     * Get the {@link Class} of the result type that will be returned by {@link #parse(String) parse}.
     * 
     * @return The result type
     */
    public Class<? extends Comparable<?>> getResultType() {
        return resultType;
    }

    /**
     * Parse {@code stringValue} to the expected type, boxed as an {@link Object} if primitive. {@link String#isEmpty()
     * Empty} inputs are always parsed to {@code null}. <em>No</em> other pre-parsing adjustments, e.g.
     * {@link String#trim() trim}, are performed on {@code stringValue} before applying type-specific parsing logic.
     *
     * @param stringValue The {@link String} to parse
     * @return The parsed result partition value as an {@code Object}
     * @throws NullPointerException if {@code stringValue} is {@code null}
     * @throws IllegalArgumentException if {@code stringValue} could not be parsed
     */
    public Comparable<?> parse(@NotNull final String stringValue) {
        if (Objects.requireNonNull(stringValue).isEmpty()) {
            return null;
        }
        try {
            return parseToType(stringValue);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("%s partition value parser for type %s failed for \"%s\"",
                    name(), resultType.getName(), stringValue));
        }
    }

    /**
     * Parse {@code stringValueTrimmed} into an instance of {@link #resultType
     * 
     * @param stringValue The (non-{@link String#isEmpty() empty}) input
     * @return The parsed result partition value as an {@code Object}
     */
    abstract Comparable<?> parseToType(@NotNull final String stringValue);

    /**
     * Look up the PartitionParser for {@code dataType} and {@code componentType}.
     *
     * @param dataType The data type {@link Class} to look up
     * @param componentType The component type {@link Class} to look up, or {@code null} if there is none
     * @return The appropriate PartitionParser, or {@code null} for unsupported {@code dataType} or
     *         {@code componentType} values
     */
    @Nullable
    public static PartitionParser lookup(@NotNull final Class<?> dataType, @Nullable final Class<?> componentType) {
        if (componentType != null) {
            return null;
        }
        return lookup(Type.find(dataType));
    }

    /**
     * Look up the PartitionParser for {@code dataType} and {@code componentType}.
     *
     * @param dataType The data type {@link Class} to look up
     * @param componentType The component type {@link Class} to look up, or {@code null} if there is none
     * @return The appropriate PartitionParser
     * @throws IllegalArgumentException for unsupported {@code dataType} values
     */
    @NotNull
    public static PartitionParser lookupSupported(
            @NotNull final Class<?> dataType,
            @Nullable final Class<?> componentType) {
        final PartitionParser supportedParser = lookup(dataType, componentType);
        if (supportedParser != null) {
            return supportedParser;
        }
        if (componentType == null) {
            throw new IllegalArgumentException(String.format(
                    "Unsupported data type %s for partition value parsing", dataType.getName()));
        }

        throw new IllegalArgumentException(String.format(
                "Unsupported data type %s or component type %s for partition value parsing",
                dataType.getName(), componentType.getName()));
    }

    /**
     * Look up the PartitionParser for {@code type}.
     *
     * @param type The {@link Type} to look up
     * @return The appropriate PartitionParser, or {@code null} for unsupported {@code type} values
     */
    @Nullable
    public static PartitionParser lookup(@NotNull final Type<?> type) {
        return type.walk(Resolver.INSTANCE);
    }

    /**
     * Look up the PartitionParser for {@code type}.
     *
     * @param type The {@link Type} to look up
     * @return The appropriate PartitionParser, or {@code null} for unsupported {@code type} values
     */
    @NotNull
    public static PartitionParser lookupSupported(@NotNull final Type<?> type) {
        final PartitionParser supportedParser = lookup(type);
        if (supportedParser != null) {
            return supportedParser;
        }
        throw new IllegalArgumentException(String.format("Unsupported type %s for partition value parsing", type));
    }

    /**
     * Visitor for resolving a {@link Type} and finding the correct PartitionParser.
     */
    private static final class Resolver implements
            Type.Visitor<PartitionParser>, GenericType.Visitor<PartitionParser>, BoxedType.Visitor<PartitionParser> {

        private static final Type.Visitor<PartitionParser> INSTANCE = new Resolver();

        private Resolver() {}

        @Override
        public PartitionParser visit(@NotNull final PrimitiveType<?> primitiveType) {
            return visit(primitiveType.boxedType());
        }

        @Override
        public PartitionParser visit(@NotNull final GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<PartitionParser>) this);
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<PartitionParser>) this);
        }

        @Override
        public PartitionParser visit(@NotNull final StringType stringType) {
            return ForString;
        }

        @Override
        public PartitionParser visit(@NotNull final InstantType instantType) {
            return ForInstant;
        }

        @Override
        public PartitionParser visit(@NotNull final ArrayType<?, ?> arrayType) {
            return null;
        }

        @Override
        public PartitionParser visit(@NotNull final CustomType<?> customType) {
            final Class<?> clazz = customType.clazz();
            if (clazz == BigInteger.class) {
                return ForBigInteger;
            }
            if (clazz == BigDecimal.class) {
                return ForBigDecimal;
            }
            if (clazz == LocalDate.class) {
                return ForLocalDate;
            }
            if (clazz == LocalTime.class) {
                return ForLocalTime;
            }
            if (clazz == ZonedDateTime.class) {
                return ForZonedDateTime;
            }
            return null;
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedBooleanType booleanType) {
            return ForBoolean;
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedByteType byteType) {
            return ForByte;
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedCharType charType) {
            return ForChar;
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedShortType shortType) {
            return ForShort;
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedIntType intType) {
            return ForInt;
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedLongType longType) {
            return ForLong;
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedFloatType floatType) {
            return ForFloat;
        }

        @Override
        public PartitionParser visit(@NotNull final BoxedDoubleType doubleType) {
            return ForDouble;
        }
    }
}
