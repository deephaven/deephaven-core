/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.ColumnToCodecMappings;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * This class provides instructions intended for read and write parquet operations (which take it as an optional
 * argument) specifying desired transformations. Examples are mapping column names and use of specific codecs during
 * (de)serialization.
 */
public abstract class ParquetInstructions implements ColumnToCodecMappings {

    private static volatile String defaultCompressionCodecName = CompressionCodecName.SNAPPY.toString();

    /**
     * Set the default for {@link #getCompressionCodecName()}.
     *
     * @deprecated Use {@link Builder#setCompressionCodecName(String)} instead.
     * @param name The new default
     */
    @Deprecated
    public static void setDefaultCompressionCodecName(final String name) {
        defaultCompressionCodecName = name;
    }

    /**
     * @return The default for {@link #getCompressionCodecName()}
     */
    public static String getDefaultCompressionCodecName() {
        return defaultCompressionCodecName;
    }

    private static volatile int defaultMaximumDictionaryKeys = 1 << 20;

    /**
     * Set the default for {@link #getMaximumDictionaryKeys()}.
     *
     * @param maximumDictionaryKeys The new default
     * @see Builder#setMaximumDictionaryKeys(int)
     */
    public static void setDefaultMaximumDictionaryKeys(final int maximumDictionaryKeys) {
        defaultMaximumDictionaryKeys = Require.geqZero(maximumDictionaryKeys, "maximumDictionaryKeys");
    }

    /**
     * @return The default for {@link #getMaximumDictionaryKeys()}
     */
    public static int getDefaultMaximumDictionaryKeys() {
        return defaultMaximumDictionaryKeys;
    }

    private static volatile int defaultMaximumDictionarySize = 1 << 20;

    /**
     * Set the default for {@link #getMaximumDictionarySize()}.
     *
     * @param maximumDictionarySize The new default
     * @see Builder#setMaximumDictionarySize(int)
     */
    public static void setDefaultMaximumDictionarySize(final int maximumDictionarySize) {
        defaultMaximumDictionarySize = Require.geqZero(maximumDictionarySize, "maximumDictionarySize");
    }

    /**
     * @return The default for {@link #getMaximumDictionarySize()}
     */
    public static int getDefaltMaximumDictionarySize() {
        return defaultMaximumDictionarySize;
    }

    public static final int MIN_TARGET_PAGE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("Parquet.minTargetPageSize", 2 << 10);
    private static final int DEFAULT_TARGET_PAGE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("Parquet.defaultTargetPageSize", 1 << 20);
    private static volatile int defaultTargetPageSize = DEFAULT_TARGET_PAGE_SIZE;

    private static final boolean DEFAULT_IS_REFRESHING = false;

    /**
     * Set the default target page size (in bytes) used to section rows of data into pages during column writing. This
     * number should be no smaller than {@link #MIN_TARGET_PAGE_SIZE}.
     *
     * @param newDefaultSizeBytes the new default target page size.
     */
    public static void setDefaultTargetPageSize(final int newDefaultSizeBytes) {
        if (newDefaultSizeBytes < MIN_TARGET_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "Default target page size should be larger than " + MIN_TARGET_PAGE_SIZE + " bytes");
        }
        defaultTargetPageSize = newDefaultSizeBytes;
    }

    /**
     * Get the current default target page size in bytes.
     * 
     * @return the current default target page size in bytes.
     */
    public static int getDefaultTargetPageSize() {
        return defaultTargetPageSize;
    }

    public ParquetInstructions() {}

    public final String getColumnNameFromParquetColumnNameOrDefault(final String parquetColumnName) {
        final String mapped = getColumnNameFromParquetColumnName(parquetColumnName);
        return (mapped != null) ? mapped : parquetColumnName;
    }

    public abstract String getParquetColumnNameFromColumnNameOrDefault(final String columnName);

    public abstract String getColumnNameFromParquetColumnName(final String parquetColumnName);

    @Override
    public abstract String getCodecName(final String columnName);

    @Override
    public abstract String getCodecArgs(final String columnName);

    /**
     * @return A hint that the writer should use dictionary-based encoding for writing this column; never evaluated for
     *         non-String columns, defaults to false
     */
    public abstract boolean useDictionary(String columnName);

    public abstract String getCompressionCodecName();

    /**
     * @return The maximum number of unique keys the writer should add to a dictionary page before switching to
     *         non-dictionary encoding; never evaluated for non-String columns, ignored if
     *         {@link #useDictionary(String)}
     */
    public abstract int getMaximumDictionaryKeys();

    /**
     * @return The maximum number of bytes the writer should add to a dictionary before switching to non-dictionary
     *         encoding; never evaluated for non-String columns, ignored if {@link #useDictionary(String)}
     */
    public abstract int getMaximumDictionarySize();

    public abstract boolean isLegacyParquet();

    public abstract int getTargetPageSize();

    /**
     * @return if the data source is refreshing
     */
    public abstract boolean isRefreshing();

    @VisibleForTesting
    public static boolean sameColumnNamesAndCodecMappings(final ParquetInstructions i1, final ParquetInstructions i2) {
        if (i1 == EMPTY) {
            if (i2 == EMPTY) {
                return true;
            }
            return ((ReadOnly) i2).columnNameToInstructions.size() == 0;
        }
        if (i2 == EMPTY) {
            return ((ReadOnly) i1).columnNameToInstructions.size() == 0;
        }
        return ReadOnly.sameCodecMappings((ReadOnly) i1, (ReadOnly) i2);
    }

    public static final ParquetInstructions EMPTY = new ParquetInstructions() {
        @Override
        public String getParquetColumnNameFromColumnNameOrDefault(final String columnName) {
            return columnName;
        }

        @Override
        public String getColumnNameFromParquetColumnName(final String parquetColumnName) {
            return null;
        }

        @Override
        public String getCodecName(final String columnName) {
            return null;
        }

        @Override
        public String getCodecArgs(final String columnName) {
            return null;
        }

        @Override
        public boolean useDictionary(final String columnName) {
            return false;
        }

        @Override
        public String getCompressionCodecName() {
            return defaultCompressionCodecName;
        }

        @Override
        public int getMaximumDictionaryKeys() {
            return defaultMaximumDictionaryKeys;
        }

        @Override
        public int getMaximumDictionarySize() {
            return defaultMaximumDictionarySize;
        }

        @Override
        public boolean isLegacyParquet() {
            return false;
        }

        @Override
        public int getTargetPageSize() {
            return defaultTargetPageSize;
        }

        @Override
        public boolean isRefreshing() {
            return DEFAULT_IS_REFRESHING;
        }
    };

    private static class ColumnInstructions {
        private final String columnName;
        private String parquetColumnName;
        private String codecName;
        private String codecArgs;
        private boolean useDictionary;

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

        public boolean useDictionary() {
            return useDictionary;
        }

        public void useDictionary(final boolean useDictionary) {
            this.useDictionary = useDictionary;
        }
    }

    private static final class ReadOnly extends ParquetInstructions {
        private final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions;
        /**
         * Note parquetColumnNameToInstructions may be null while columnNameToInstructions is not null; We only store
         * entries in parquetColumnNameToInstructions when the parquetColumnName is different than the columnName (ie,
         * the column name mapping is not the default mapping)
         */
        private final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToInstructions;
        private final String compressionCodecName;
        final private int maximumDictionaryKeys;
        final private int maximumDictionarySize;
        private final boolean isLegacyParquet;
        private final int targetPageSize;
        private final boolean isRefreshing;

        private ReadOnly(
                final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions,
                final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToColumnName,
                final String compressionCodecName,
                final int maximumDictionaryKeys,
                final int maximumDictionarySize,
                final boolean isLegacyParquet,
                final int targetPageSize,
                final boolean isRefreshing) {
            this.columnNameToInstructions = columnNameToInstructions;
            this.parquetColumnNameToInstructions = parquetColumnNameToColumnName;
            this.compressionCodecName = compressionCodecName;
            this.maximumDictionaryKeys = maximumDictionaryKeys;
            this.maximumDictionarySize = maximumDictionarySize;
            this.isLegacyParquet = isLegacyParquet;
            this.targetPageSize = targetPageSize;
            this.isRefreshing = isRefreshing;
        }

        private String getOrDefault(final String columnName, final String defaultValue,
                final Function<ColumnInstructions, String> fun) {
            if (columnNameToInstructions == null) {
                return defaultValue;
            }
            final ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci == null) {
                return defaultValue;
            }
            return fun.apply(ci);
        }

        private boolean getOrDefault(final String columnName, final boolean defaultValue,
                final Predicate<ColumnInstructions> fun) {
            if (columnNameToInstructions == null) {
                return defaultValue;
            }
            final ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci == null) {
                return defaultValue;
            }
            return fun.test(ci);
        }

        @Override
        public String getParquetColumnNameFromColumnNameOrDefault(final String columnName) {
            return getOrDefault(columnName, columnName, ColumnInstructions::getParquetColumnName);
        }

        @Override
        public String getColumnNameFromParquetColumnName(final String parquetColumnName) {
            if (parquetColumnNameToInstructions == null) {
                return null;
            }
            final ColumnInstructions ci = parquetColumnNameToInstructions.get(parquetColumnName);
            if (ci == null) {
                return null;
            }
            return ci.getColumnName();
        }

        @Override
        public String getCodecName(final String columnName) {
            return getOrDefault(columnName, null, ColumnInstructions::getCodecName);
        }

        @Override
        public String getCodecArgs(final String columnName) {
            return getOrDefault(columnName, null, ColumnInstructions::getCodecArgs);
        }

        @Override
        public boolean useDictionary(final String columnName) {
            return getOrDefault(columnName, false, ColumnInstructions::useDictionary);
        }

        @Override
        public String getCompressionCodecName() {
            return compressionCodecName;
        }

        @Override
        public int getMaximumDictionaryKeys() {
            return maximumDictionaryKeys;
        }

        @Override
        public int getMaximumDictionarySize() {
            return maximumDictionarySize;
        }

        @Override
        public boolean isLegacyParquet() {
            return isLegacyParquet;
        }

        @Override
        public int getTargetPageSize() {
            return targetPageSize;
        }

        @Override
        public boolean isRefreshing() {
            return isRefreshing;
        }

        KeyedObjectHashMap<String, ColumnInstructions> copyColumnNameToInstructions() {
            // noinspection unchecked
            return (columnNameToInstructions == null)
                    ? null
                    : (KeyedObjectHashMap<String, ColumnInstructions>) columnNameToInstructions.clone();
        }

        KeyedObjectHashMap<String, ColumnInstructions> copyParquetColumnNameToInstructions() {
            // noinspection unchecked
            return (parquetColumnNameToInstructions == null)
                    ? null
                    : (KeyedObjectHashMap<String, ColumnInstructions>) parquetColumnNameToInstructions.clone();
        }

        private static boolean sameCodecMappings(final ReadOnly r1, final ReadOnly r2) {
            final Set<String> r1ColumnNames = r1.columnNameToInstructions.keySet();
            if (r2.columnNameToInstructions.size() != r1ColumnNames.size()) {
                return false;
            }
            for (String colName : r1ColumnNames) {
                if (!r2.columnNameToInstructions.containsKey(colName)) {
                    return false;
                }
                final String r1CodecName = r1.getCodecName(colName);
                final String r2CodecName = r2.getCodecName(colName);
                if (!Objects.equals(r1CodecName, r2CodecName)) {
                    return false;
                }
                final String r1CodecArgs = r1.getCodecArgs(colName);
                final String r2CodecArgs = r2.getCodecArgs(colName);
                if (!Objects.equals(r1CodecArgs, r2CodecArgs)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class Builder {
        private KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions;
        // Note parquetColumnNameToInstructions may be null while columnNameToInstructions is not null;
        // We only store entries in parquetColumnNameToInstructions when the parquetColumnName is
        // different than the columnName (ie, the column name mapping is not the default mapping)
        private KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToInstructions;
        private String compressionCodecName = defaultCompressionCodecName;
        private int maximumDictionaryKeys = defaultMaximumDictionaryKeys;
        private int maximumDictionarySize = defaultMaximumDictionarySize;
        private boolean isLegacyParquet;
        private int targetPageSize = defaultTargetPageSize;
        private boolean isRefreshing = DEFAULT_IS_REFRESHING;

        public Builder() {}

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
            parquetColumnNameToInstructions =
                    new KeyedObjectHashMap<>(new KeyedObjectKey.Basic<String, ColumnInstructions>() {
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
                                    + ": columnName=" + columnName + " already mapped to parquetColumnName="
                                    + ci.parquetColumnName);
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

            final ColumnInstructions fromParquetColumnNameInstructions =
                    parquetColumnNameToInstructions.get(parquetColumnName);
            if (fromParquetColumnNameInstructions != null) {
                if (fromParquetColumnNameInstructions == ci) {
                    return this;
                }
                throw new IllegalArgumentException(
                        "Cannot add new mapping from parquetColumnName=" + parquetColumnName + " to columnName="
                                + columnName
                                + ": already mapped to columnName="
                                + fromParquetColumnNameInstructions.getColumnName());
            }
            ci.setParquetColumnName(parquetColumnName);
            parquetColumnNameToInstructions.put(parquetColumnName, ci);
            return this;
        }

        public Set<String> getTakenNames() {
            return (columnNameToInstructions == null) ? Collections.emptySet() : columnNameToInstructions.keySet();
        }

        public Builder addColumnCodec(final String columnName, final String codecName) {
            return addColumnCodec(columnName, codecName, null);
        }

        public Builder addColumnCodec(final String columnName, final String codecName, final String codecArgs) {
            final ColumnInstructions ci = getColumnInstructions(columnName);
            ci.setCodecName(codecName);
            ci.setCodecArgs(codecArgs);
            return this;
        }

        /**
         * Set a hint that the writer should use dictionary-based encoding for writing this column; never evaluated for
         * non-String columns.
         *
         * @param columnName The column name
         * @param useDictionary The hint value
         */
        public Builder useDictionary(final String columnName, final boolean useDictionary) {
            final ColumnInstructions ci = getColumnInstructions(columnName);
            ci.useDictionary(useDictionary);
            return this;
        }

        private ColumnInstructions getColumnInstructions(final String columnName) {
            final ColumnInstructions ci;
            if (columnNameToInstructions == null) {
                newColumnNameToInstructionsMap();
                ci = new ColumnInstructions(columnName);
                columnNameToInstructions.put(columnName, ci);
            } else {
                ci = columnNameToInstructions.putIfAbsent(columnName, ColumnInstructions::new);
            }
            return ci;
        }

        public Builder setCompressionCodecName(final String compressionCodecName) {
            this.compressionCodecName = compressionCodecName;
            return this;
        }

        /**
         * Set the maximum number of unique keys the writer should add to a dictionary page before switching to
         * non-dictionary encoding; never evaluated for non-String columns, ignored if {@link #useDictionary(String) use
         * dictionary} is set for the column.
         *
         * @param maximumDictionaryKeys The maximum number of dictionary keys; must be {@code >= 0}
         */
        public Builder setMaximumDictionaryKeys(final int maximumDictionaryKeys) {
            this.maximumDictionaryKeys = Require.geqZero(maximumDictionaryKeys, "maximumDictionaryKeys");
            return this;
        }

        /**
         * Set the maximum number of bytes the writer should add to the dictionary before switching to non-dictionary
         * encoding; never evaluated for non-String columns, ignored if {@link #useDictionary(String) use dictionary} is
         * set for the column.
         *
         * @param maximumDictionarySize The maximum size of dictionary (in bytes); must be {@code >= 0}
         */
        public Builder setMaximumDictionarySize(final int maximumDictionarySize) {
            this.maximumDictionarySize = Require.geqZero(maximumDictionarySize, "maximumDictionarySize");
            return this;
        }

        public Builder setIsLegacyParquet(final boolean isLegacyParquet) {
            this.isLegacyParquet = isLegacyParquet;
            return this;
        }

        public Builder setTargetPageSize(final int targetPageSize) {
            if (targetPageSize < MIN_TARGET_PAGE_SIZE) {
                throw new IllegalArgumentException("Target page size should be >= " + MIN_TARGET_PAGE_SIZE);
            }
            this.targetPageSize = targetPageSize;
            return this;
        }

        public Builder setIsRefreshing(final boolean isRefreshing) {
            this.isRefreshing = isRefreshing;
            return this;
        }

        public ParquetInstructions build() {
            final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructionsOut = columnNameToInstructions;
            columnNameToInstructions = null;
            final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToColumnNameOut =
                    parquetColumnNameToInstructions;
            parquetColumnNameToInstructions = null;
            return new ReadOnly(columnNameToInstructionsOut, parquetColumnNameToColumnNameOut, compressionCodecName,
                    maximumDictionaryKeys, maximumDictionarySize, isLegacyParquet, targetPageSize, isRefreshing);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
