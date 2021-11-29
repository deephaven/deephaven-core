package io.deephaven.engine.table.impl.preview;

import io.deephaven.vector.Vector;
import io.deephaven.vector.VectorFactory;
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
        return new ArrayPreview(VectorFactory.forElementType(array.getClass().getComponentType()).vectorWrap(array)
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
}
