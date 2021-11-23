package io.deephaven.engine.util;

import java.util.ArrayList;

/**
 * Helper class to support easier to read in-line column definitions. You can obtain the array arguments to feed the
 * buildWriter methods in TableWriterFactory.
 */
public class ColumnsSpecHelper {

    private final ArrayList<String> names = new ArrayList<>();
    private final ArrayList<Class> types = new ArrayList<>();

    public ColumnsSpecHelper add(final String name, final Class type) {
        names.add(name);
        types.add(type);
        return this;
    }

    public String[] getColumnNames() {
        return names.toArray(new String[0]);
    }

    public Class[] getTypes() {
        return types.toArray(new Class[0]);
    }
}
