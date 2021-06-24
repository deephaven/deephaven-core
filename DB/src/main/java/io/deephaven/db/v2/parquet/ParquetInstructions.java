package io.deephaven.db.v2.parquet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class ParquetInstructions {
    public ParquetInstructions() {
    }

    public final String getColumnNameFromParquetColumnNameOrDefault(final String parquetColumnName) {
        final String mapped = getColumnNameFromParquetColumnName(parquetColumnName);
        return (mapped != null) ? mapped : parquetColumnName;
    }
    public abstract String getParquetColumnNameFromColumnNameOrDefault(final String columnName);
    public abstract String getColumnNameFromParquetColumnName(final String parquetColumnName);

    public static final ParquetInstructions EMPTY = new ParquetInstructions() {
        @Override
        public String getParquetColumnNameFromColumnNameOrDefault(final String columnName) {
            return columnName;
        }
        @Override
        public String getColumnNameFromParquetColumnName(final String parquetColumnName) {
            return null;
        }
    };

    private static class ReadOnly extends ParquetInstructions {
        private final Map<String, ColumnInstructions> columnNameToInstructions;
        private final Map<String, String> parquetColumnNameToColumnName;

        protected ReadOnly(
                final Map<String, ColumnInstructions> columnNameToInstructions,
                final Map<String, String> parquetColumnNameToColumnName) {
            this.columnNameToInstructions = columnNameToInstructions != null
                    ? Collections.unmodifiableMap(columnNameToInstructions)
                    : null
                    ;
            this.parquetColumnNameToColumnName = parquetColumnNameToColumnName != null
                    ? Collections.unmodifiableMap(parquetColumnNameToColumnName)
                    : null
                    ;
        }

        @Override
        public String getParquetColumnNameFromColumnNameOrDefault(final String columnName) {
            if (columnNameToInstructions == null) {
                return columnName;
            }
            final ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci == null) {
                return columnName;
            }
            return ci.getParquetColumnName();
        }

        @Override
        public String getColumnNameFromParquetColumnName(final String parquetColumnName) {
            if (parquetColumnNameToColumnName == null) {
                return null;
            }
            return parquetColumnNameToColumnName.get(parquetColumnName);
        }
    }

    public static class Builder {
        private Map<String, ColumnInstructions> columnNameToInstructions;
        private Map<String, String> parquetColumnNameToColumnName;

        public Builder() {
        }

        public Builder(final ParquetInstructions parquetInstructions) {
            if (parquetInstructions == EMPTY) {
                return;
            }
            final ReadOnly readOnlyParquetInstructions = (ReadOnly) parquetInstructions;
            columnNameToInstructions = readOnlyParquetInstructions.columnNameToInstructions == null
                    ? null
                    : new HashMap<>(readOnlyParquetInstructions.columnNameToInstructions);
            parquetColumnNameToColumnName = readOnlyParquetInstructions.parquetColumnNameToColumnName == null
                    ? null
                    : new HashMap<>(readOnlyParquetInstructions.parquetColumnNameToColumnName);
        }
        public Builder addColumnNameMapping(final String parquetColumnName, final String columnName) {
            if (columnNameToInstructions == null) {
                columnNameToInstructions = new HashMap<>();
                parquetColumnNameToColumnName = new HashMap<>();
            }
            ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci != null) {
                if (ci.parquetColumnName != null) {
                    if (ci.parquetColumnName.equals(parquetColumnName)) {
                        return this;
                    }
                    throw new IllegalArgumentException(
                            "Cannot add a mapping to already mapped parqueColumnName=" + ci.parquetColumnName + " for column=" + columnName);
                }
            } else {
                ci = new ColumnInstructions(columnName);
                columnNameToInstructions.put(columnName, ci);

            }
            ci.setParquetColumnName(parquetColumnName);
            parquetColumnNameToColumnName.put(parquetColumnName, columnName);
            return this;
        }

        public ParquetInstructions build() {
            final Map<String, ColumnInstructions> columnNameToInstructionsOut = columnNameToInstructions;
            columnNameToInstructions = null;
            final Map<String, String> parquetColumnNameToColumnNameOut = parquetColumnNameToColumnName;
            parquetColumnNameToColumnName = null;
            return new ReadOnly(columnNameToInstructionsOut, parquetColumnNameToColumnNameOut);
        }
    }
}
