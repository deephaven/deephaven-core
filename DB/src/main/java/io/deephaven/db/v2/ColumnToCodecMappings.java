package io.deephaven.db.v2;

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
