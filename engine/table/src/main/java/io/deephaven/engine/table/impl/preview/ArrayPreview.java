//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.preview;

import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import io.deephaven.vector.VectorFactory;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyListWrapper;

/**
 * A Preview Type for Arrays and Vector. Converts long arrays to a String "[1, 2, 3, 4, 5...]"
 */
public class ArrayPreview implements PreviewType {
    private static final int ARRAY_SIZE_CUTOFF = 5;
    private final String displayString;

    public static ArrayPreview fromVector(final Vector<?> vector) {
        if (vector == null) {
            return null;
        }
        return new ArrayPreview(vector.toString(ARRAY_SIZE_CUTOFF));
    }

    public static ArrayPreview fromArray(final Object array) {
        if (array == null) {
            return null;
        }
        if (!array.getClass().isArray()) {
            throw new IllegalArgumentException("Input must be an array, instead input class is " + array.getClass());
        }
        final Class<?> componentType = array.getClass().getComponentType();
        // we do not support boolean[] as a wrapped Vector
        if (componentType == boolean.class) {
            return new ArrayPreview(convertToString((boolean[]) array));
        }
        // Boxed primitives need the Object wrapper.
        final Class<?> elementType = TypeUtils.isBoxedType(componentType) ? Object.class : componentType;
        return new ArrayPreview(VectorFactory.forElementType(elementType)
                .vectorWrap(array)
                .toString(ARRAY_SIZE_CUTOFF));
    }

    public static ArrayPreview fromPyListWrapper(final PyListWrapper list) {
        if (list == null) {
            return null;
        }
        return new ArrayPreview(list.toString(ARRAY_SIZE_CUTOFF));
    }

    private ArrayPreview(String displayString) {
        this.displayString = displayString;

    }

    @Override
    public String toString() {
        return displayString;
    }

    /**
     * Helper method for generating a string preview of a {@code boolean[]}.
     *
     * @param array The boolean array to convert to a String
     * @return The String representation of array
     */
    private static String convertToString(@NotNull final boolean[] array) {
        if (array.length == 0) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = Math.min(array.length, ARRAY_SIZE_CUTOFF);
        builder.append(array[0] ? "true" : "false");
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(array[ei] ? "true" : "false");
        }
        if (displaySize == array.length) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }
}
