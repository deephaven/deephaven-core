package io.deephaven.db.tables.utils;

import java.util.ArrayList;

/**
 * Helper class to support easier to read in-line column definitions. You can obtain the array
 * arguments to feed the buildWriter methods in TableWriterFactory.
 */
public class ColumnsSpecHelper {
    private final ArrayList<String> names = new ArrayList<>();
    private final ArrayList<Class> dbTypes = new ArrayList<>();

    public ColumnsSpecHelper add(final String name, final Class dbType) {
        names.add(name);
        dbTypes.add(dbType);
        return this;
    }

    public String[] getColumnNames() {
        return names.toArray(new String[names.size()]);
    }

    public Class[] getDbTypes() {
        return dbTypes.toArray(new Class[dbTypes.size()]);
    }
}
