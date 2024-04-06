//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import java.util.Collection;

public interface MultiJoinTable {
    /**
     * Get the output {@link Table table} from this multi-join table.
     *
     * @return The output {@link Table table}
     */
    Table table();

    /**
     * Get the key column names from this multi-join table.
     *
     * @return The key column names as a collection of strings
     */
    Collection<String> keyColumns();
}
