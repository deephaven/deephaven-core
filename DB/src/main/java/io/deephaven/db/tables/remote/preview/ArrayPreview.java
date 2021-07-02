package io.deephaven.db.tables.remote.preview;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.chunk.ChunkType;

/**
 * A Preview Type for Arrays and DbArray.  Converts long arrays to a String "[1, 2, 3, 4, 5...]"
 */
public class ArrayPreview implements PreviewType {
    private static final int ARRAY_SIZE_CUTOFF = 5;
    private final String displayString;

    public static ArrayPreview fromDbArray(DbArrayBase dbArray) {
        if(dbArray == null) {
            return null;
        }
        return new ArrayPreview(dbArray.toString(ARRAY_SIZE_CUTOFF));
    }

    public static ArrayPreview fromArray(Object array) {
        if(array == null) {
            return null;
        }
        if (!array.getClass().isArray()) {
            throw new IllegalArgumentException("Input must be an array, instead input class is " + array.getClass());
        }
        return new ArrayPreview(ChunkType.fromElementType(array.getClass().getComponentType()).dbArrayWrap(array).toString(ARRAY_SIZE_CUTOFF));
    }

    private ArrayPreview(String displayString) {
        this.displayString = displayString;

    }

    @Override
    public String toString() {
        return displayString;
    }
}
