package io.deephaven.db.v2.parquet;

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

    private static class Base extends ParquetInstructions {
        private Map<String, ColumnInstructions> columnNameToInstructions;
        private Map<String, String> parquetColumnNameToColumnName;

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

        public ParquetInstructions addColumnNameMapping(final String parquetColumnName, final String columnName) {
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
    }

    public static final class Read extends Base {
        @Override
        public Read addColumnNameMapping(final String parquetColumnName, final String columnName) {
            super.addColumnNameMapping(parquetColumnName, columnName);
            return this;
        }
    }

    public static final class Write extends Base {
        @Override
        public Write addColumnNameMapping(final String parquetColumnName, final String columnName) {
            super.addColumnNameMapping(parquetColumnName, columnName);
            return this;
        }
    }
}
