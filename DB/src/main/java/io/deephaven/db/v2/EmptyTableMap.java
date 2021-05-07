package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;

import java.util.Collections;

/**
 * "Empty" table map class.
 */
class EmptyTableMap extends LocalTableMap {

    static final TableMap INSTANCE = new EmptyTableMap();

    private EmptyTableMap() {
        super(Collections.emptyMap(), null, null);
    }

    @Override
    public final synchronized Table put(Object key, Table table) {
        throw new UnsupportedOperationException("EmptyTableMap does not support put");
    }
}
