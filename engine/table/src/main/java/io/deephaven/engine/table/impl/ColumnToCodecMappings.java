/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

public interface ColumnToCodecMappings {
    String getCodecName(final String columnName);

    String getCodecArgs(final String columnName);

    ColumnToCodecMappings EMPTY = new ColumnToCodecMappings() {
        @Override
        public String getCodecName(final String columnName) {
            return null;
        }

        @Override
        public String getCodecArgs(final String columnName) {
            return null;
        }
    };
}
