package io.deephaven.util.type;

import java.util.Arrays;

public class EnumValue {

    /**
     * Retrieves the enum constant whose name matches a given value according to case-insensitive comparison.
     *
     * @param enumClass the enum type we are querying
     * @param value the constant value we are looking up
     * @return the enum constant
     * @throws IllegalArgumentException when value is not found in the Enum's constants
     */
    public static <T extends Enum<?>> T caseInsensitiveValueOf(Class<T> enumClass, String value) {
        return Arrays.stream(enumClass.getEnumConstants()).filter(x -> x.name().equalsIgnoreCase(value)).findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException(
                                enumClass.getSimpleName() + " has no constant that matches " + value));
    }
}
