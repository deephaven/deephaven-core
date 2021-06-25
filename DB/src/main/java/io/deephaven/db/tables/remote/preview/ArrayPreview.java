package io.deephaven.db.tables.remote.preview;

import io.deephaven.db.tables.dbarrays.DbArrayBase;

import java.lang.reflect.Array;
import java.util.function.Function;

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

        final String displayString;
//        if(dbArray.size() <= ARRAY_SIZE_CUTOFF) {
            displayString = dbArray.toString();
//        } else {
//            displayString = dbArray.subArray(0, ARRAY_SIZE_CUTOFF).toString().replace("]", ", ...]");
//        }
        return new ArrayPreview(displayString);
    }

    public static ArrayPreview fromArray(Object array) {
        if(array == null) {
            return null;
        }

        final int arrayLength = Array.getLength(array);
        final int length = Math.min(arrayLength, ARRAY_SIZE_CUTOFF);

        if (length == 0) {
            return new ArrayPreview("[]");
        }

        final Function<Object, String> valToString = DbArrayBase.classToHelper(array.getClass().getComponentType());

        final StringBuilder builder = new StringBuilder("[");
        builder.append(valToString.apply(Array.get(array, 0)));
        for(int ei = 1; ei < length; ++ei) {
            builder.append(',').append(valToString.apply(Array.get(array, ei)));
        }
        if(arrayLength > ARRAY_SIZE_CUTOFF) {
            builder.append(", ...]");
        } else {
            builder.append("]");
        }
        return new ArrayPreview(builder.toString());
    }

    private ArrayPreview(String displayString) {
        this.displayString = displayString;

    }

    @Override
    public String toString() {
        return displayString;
    }
}
