package io.deephaven.db.v2.parquet;

import java.util.HashMap;
import java.util.Map;

public abstract class ParquetInstructions {
    public ParquetInstructions() {
    }

    public abstract String getColumnNameFromParquetColumnName(final String parquetColumnName);
    public abstract String getParquetColumnNameFromColumnName(final String columnName);

    public static final ParquetInstructions EMPTY = new ParquetInstructions() {
        @Override
        public String getColumnNameFromParquetColumnName(final String parquetColumnName) {
            return parquetColumnName;
        }

        @Override
        public String getParquetColumnNameFromColumnName(final String columnName) {
            return columnName;
        }
    };

    private static class Base extends ParquetInstructions {
        private Map<String, ColumnInstructions> columnNameToInstructions;
        private Map<String, String> parquetColumnNameToColumnName;

        public String getColumnNameFromParquetColumnName(final String parquetColumnName) {
            if (parquetColumnNameToColumnName == null) {
                return parquetColumnName;
            }
            final String mapping = parquetColumnNameToColumnName.get(parquetColumnName);
            if (mapping != null) {
                return mapping;
            }
            return parquetColumnName;
        }

        public String getParquetColumnNameFromColumnName(final String columnName) {
            if (columnNameToInstructions == null) {
                return columnName;
            }
            final ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci == null) {
                return columnName;
            }
            return ci.getParquetColumnName();
        }

        public ParquetInstructions addColumnNameMapping(final String parquetColumnName, final String columnName) {
            if (columnNameToInstructions == null) {
                columnNameToInstructions = new HashMap<>();
                parquetColumnNameToColumnName = new HashMap<>();
            }
            ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci != null) {
                final String previous = ci.getParquetColumnName();
                if (previous != null) {
                    if (previous.equals(parquetColumnName)) {
                        return this;
                    }
                    throw new IllegalArgumentException(
                            "Cannot add a mapping to already mapped parqueColumnName=" + previous + " for column=" + columnName);
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
