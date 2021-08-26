package io.deephaven.db.tables.remote.preview;

import io.deephaven.configuration.Configuration;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/**
 * Wraps Objects that cannot be displayed (e.g. not serializable or an unknown class) and allows
 * them to be displayed as a String.
 */
public class DisplayWrapper implements Serializable {
    private static final int MAX_CHARACTERS =
        Configuration.getInstance().getIntegerWithDefault("DisplayWrapper.maxCharacters", 10000);
    private final String displayString;

    /**
     * Creates a new DisplayWrapper around an Object. Returns null if the Object is null.
     *
     * @param objectToWrap the Object to wrap
     * @return a new DisplayWrapper, null if the Object is null
     */
    @Nullable
    public static DisplayWrapper make(Object objectToWrap) {
        if (objectToWrap == null) {
            return null;
        } else {
            return new DisplayWrapper(objectToWrap);
        }
    }

    private DisplayWrapper(Object objectToWrap) {
        final String s = objectToWrap.toString();
        displayString = s.substring(0, Math.min(s.length(), MAX_CHARACTERS));
    }

    @Override
    public String toString() {
        return displayString;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DisplayWrapper) {
            return displayString.equals(((DisplayWrapper) obj).displayString);
        }

        return false;
    }
}
