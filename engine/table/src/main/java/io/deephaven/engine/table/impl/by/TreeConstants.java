/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;

/**
 * Implementation constants for {@link io.deephaven.engine.table.Table#tree} support.
 */
public final class TreeConstants {

    private TreeConstants() {}

    public static final ColumnName SOURCE_ROW_LOOKUP_ROW_KEY_COLUMN = ColumnName.of("__ROW_KEY__");
}
