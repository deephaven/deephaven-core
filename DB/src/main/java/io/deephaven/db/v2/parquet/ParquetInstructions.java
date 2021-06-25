package io.deephaven.db.v2.parquet;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;

/**
 * This class provides instructions intended for read and write parquet operations (which take
 * it as an optional argument) specifying desired transformations.  Examples are
 * mapping column names and use of specific codecs during (de)serialization.
 */
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

    private static class ColumnInstructions {
        private final String columnName;
        private String parquetColumnName;
        private String codecName;
        private String codecArgs;

        public ColumnInstructions(final String columnName) {
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getParquetColumnName() {
            return parquetColumnName != null ? parquetColumnName : columnName;
        }
        public ColumnInstructions setParquetColumnName(final String parquetColumnName) {
            this.parquetColumnName = parquetColumnName;
            return this;
        }

        public String getCodecName() {
            return codecName;
        }
        public ColumnInstructions setCodecName(final String codecName) {
            this.codecName = codecName;
            return this;
        }

        public String getCodecArgs() {
            return codecArgs;
        }
        public ColumnInstructions setCodecArgs(final String codecArgs) {
            this.codecArgs = codecArgs;
            return this;
        }
    }

    private static class ReadOnly extends ParquetInstructions {
        private final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions;
        /**
         * Note parquetColumnNameToInstructions may be null while columnNameToInstructions is not null;
         * We only store entries in parquetColumnNameToInstructions when the parquetColumnName is
         * different than the columnName (ie, the column name mapping is not the default mapping)
         */
        private final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToInstructions;

        protected ReadOnly(
                final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions,
                final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToColumnName) {
            this.columnNameToInstructions = columnNameToInstructions;
            this.parquetColumnNameToInstructions = parquetColumnNameToColumnName;
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
            if (parquetColumnNameToInstructions == null) {
                return parquetColumnName;
            }
            final ColumnInstructions ci = parquetColumnNameToInstructions.get(parquetColumnName);
            if (ci == null) {
                return parquetColumnName;
            }
            return ci.getColumnName();
        }

        KeyedObjectHashMap<String, ColumnInstructions> copyColumnNameToInstructions() {
            // noinspection unchecked
            return (columnNameToInstructions == null)
                    ? null
                    : (KeyedObjectHashMap<String, ColumnInstructions>) columnNameToInstructions.clone()
                    ;
        }

        KeyedObjectHashMap<String, ColumnInstructions> copyParquetColumnNameToInstructions() {
            // noinspection unchecked
            return (parquetColumnNameToInstructions == null)
                    ? null
                    : (KeyedObjectHashMap<String, ColumnInstructions>) parquetColumnNameToInstructions.clone()
                    ;
        }
    }

    public static class Builder {
        private KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions;
        // Note parquetColumnNameToInstructions may be null while columnNameToInstructions is not null;
        // We only store entries in parquetColumnNameToInstructions when the parquetColumnName is
        // different than the columnName (ie, the column name mapping is not the default mapping)
        private KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToInstructions;

        public Builder() {
        }

        public Builder(final ParquetInstructions parquetInstructions) {
            if (parquetInstructions == EMPTY) {
                return;
            }
            final ReadOnly readOnlyParquetInstructions = (ReadOnly) parquetInstructions;
            columnNameToInstructions = readOnlyParquetInstructions.copyColumnNameToInstructions();
            parquetColumnNameToInstructions = readOnlyParquetInstructions.copyParquetColumnNameToInstructions();
        }

        private void newColumnNameToInstructionsMap() {
            columnNameToInstructions = new KeyedObjectHashMap<>(new KeyedObjectKey.Basic<String, ColumnInstructions>() {
                @Override
                public String getKey(@NotNull final ColumnInstructions value) {
                    return value.getColumnName();
                }
            });
        }

        private void newParquetColumnNameToInstructionsMap() {
            parquetColumnNameToInstructions = new KeyedObjectHashMap<>(new KeyedObjectKey.Basic<String, ColumnInstructions>() {
                @Override
                public String getKey(@NotNull final ColumnInstructions value) {
                    return value.getParquetColumnName();
                }
            });
        }

        public Builder addColumnNameMapping(final String parquetColumnName, final String columnName) {
            if (parquetColumnName.equals(columnName)) {
                return this;
            }
            if (columnNameToInstructions == null) {
                newColumnNameToInstructionsMap();
                final ColumnInstructions ci = new ColumnInstructions(columnName);
                ci.setParquetColumnName(parquetColumnName);
                columnNameToInstructions.put(columnName, ci);
                newParquetColumnNameToInstructionsMap();
                parquetColumnNameToInstructions.put(parquetColumnName, ci);
                return this;
            }

            ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci != null) {
                if (ci.parquetColumnName != null) {
                    if (ci.parquetColumnName.equals(parquetColumnName)) {
                        return this;
                    }
                    throw new IllegalArgumentException(
                            "Cannot add a mapping from parquetColumnName=" + parquetColumnName
                            + ": columnName=" + columnName + " already mapped to parquetColumnName=" + ci.parquetColumnName);
                }
            } else {
                ci = new ColumnInstructions(columnName);
                columnNameToInstructions.put(columnName, ci);
            }

            if (parquetColumnNameToInstructions == null) {
                newParquetColumnNameToInstructionsMap();
                parquetColumnNameToInstructions.put(parquetColumnName, ci);
                return this;
            }

            final ColumnInstructions fromParquetColumnNameInstructions = parquetColumnNameToInstructions.get(parquetColumnName);
            if (fromParquetColumnNameInstructions != null) {
                if (fromParquetColumnNameInstructions.getColumnName().equals(columnName)) {
                    return this;
                }
                throw new IllegalArgumentException(
                        "Cannot add new mapping from parquetColumnName=" + parquetColumnName + " to columnName=" + columnName
                                + ": already mapped to columnName=" + fromParquetColumnNameInstructions.getColumnName());
            }
            ci.setParquetColumnName(parquetColumnName);
            parquetColumnNameToInstructions.put(parquetColumnName, ci);
            return this;
        }

        public ParquetInstructions build() {
            final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructionsOut = columnNameToInstructions;
            columnNameToInstructions = null;
            final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToColumnNameOut = parquetColumnNameToInstructions;
            parquetColumnNameToInstructions = null;
            return new ReadOnly(columnNameToInstructionsOut, parquetColumnNameToColumnNameOut);
        }
    }
}
