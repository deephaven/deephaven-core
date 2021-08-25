/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util.string;

import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.string.cache.*;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.util.caching.C14nUtil;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class StringUtils implements Serializable {

    private static final int STRING_CACHE_SIZE = Configuration.getInstance().getInteger("StringUtils.cacheSize");

    // ------------------------------------------------------------------------------------------------------------------
    // A thread-safe (but not very concurrent) StringCache for use in Deephaven code that desires actual caching
    // ------------------------------------------------------------------------------------------------------------------

    public static final StringCache<String> STRING_CACHE =
            C14nUtil.ENABLED
                    ? new OpenAddressedWeakUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE,
                            C14nUtil.CACHE)
                    : STRING_CACHE_SIZE == 0 ? AlwaysCreateStringCache.STRING_INSTANCE
                            : new ConcurrentBoundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE,
                                    STRING_CACHE_SIZE, 2);

    // ------------------------------------------------------------------------------------------------------------------
    // Optional use of CompressedStrings for Symbol or String data columns (excluding partitioning columns)
    // ------------------------------------------------------------------------------------------------------------------

    private static final boolean USE_COMPRESSED_STRINGS =
            Configuration.getInstance().getBooleanWithDefault("StringUtils.useCompressedStrings", false);

    public static final StringCache<CompressedString> COMPRESSED_STRING_CACHE =
            USE_COMPRESSED_STRINGS
                    ? C14nUtil.ENABLED
                            ? new OpenAddressedWeakUnboundedStringCache<>(
                                    StringCacheTypeAdapterCompressedStringImpl.INSTANCE, C14nUtil.CACHE)
                            : STRING_CACHE_SIZE == 0 ? AlwaysCreateStringCache.COMPRESSED_STRING_INSTANCE
                                    : new ConcurrentBoundedStringCache<>(
                                            StringCacheTypeAdapterCompressedStringImpl.INSTANCE, STRING_CACHE_SIZE, 2)
                    : null;

    /**
     * Re-write all non-partitioning String columns in the definition to use CompressedString data type. Note this
     * should only be called when it's known that the source will only provide String data with single-byte encodings.
     *
     * @param tableDefinition table definition
     * @return the new table definition
     */
    public static TableDefinition rewriteStringColumnTypes(final TableDefinition tableDefinition) {
        if (!USE_COMPRESSED_STRINGS) {
            return tableDefinition;
        }
        final ColumnDefinition<?>[] resultColumns =
                Arrays.copyOf(tableDefinition.getColumns(), tableDefinition.getColumns().length);
        for (int ci = 0; ci < resultColumns.length; ++ci) {
            final ColumnDefinition<?> column = resultColumns[ci];
            if (column.getDataType() == String.class
                    && column.getColumnType() != ColumnDefinition.COLUMNTYPE_PARTITIONING) {
                resultColumns[ci] = column.withDataType(CompressedString.class);
            }
        }
        return new TableDefinition(resultColumns);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Other utilities
    // ------------------------------------------------------------------------------------------------------------------

    public static Collection<String> splitToCollection(String string) {
        return string.trim().isEmpty() ? Collections.emptyList()
                : Arrays.stream(string.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
    }

    /**
     * Convenience method to combine null and isEmpty checks on a String.
     *
     * @param s a String, possibly null
     * @return true if the string is null or is empty
     */
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public static <STRING_LIKE_TYPE extends CharSequence> StringCache<STRING_LIKE_TYPE> getStringCache(
            Class<STRING_LIKE_TYPE> dataType) {
        if (String.class == dataType) {
            return (StringCache<STRING_LIKE_TYPE>) StringUtils.STRING_CACHE;
        } else if (CompressedString.class == dataType) {
            return (StringCache<STRING_LIKE_TYPE>) StringUtils.COMPRESSED_STRING_CACHE;
        } else {
            // Writing code has been updated to support arbitrary CharSequences without reverting to
            // Externalizable/Serializable implementations. Reading code doesn't know what to do with other
            // CharSequence types, especially given that most are mutable.
            throw new IllegalArgumentException("Unsupported CharSequence type " + dataType);
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // String/CharSequence KeyedObjectKey implementation
    // ------------------------------------------------------------------------------------------------------------------

    /**
     * Generic String -> Object key representation.
     * 
     * @param <VALUE_TYPE>
     */
    private static class StringKey<VALUE_TYPE> extends KeyedObjectKey.Basic<String, VALUE_TYPE> {

        private static KeyedObjectKey.Basic<String, ?> INSTANCE = new StringKey<>();

        @Override
        public String getKey(@NotNull final VALUE_TYPE value) {
            return value.toString();
        }
    }

    /**
     * Generic accessor for the singleton StringKey instance.
     * 
     * @param <VALUE_TYPE> The type of the value for this key implementation
     * @return A String->StringKeyedObject key representation instance.
     */
    @SuppressWarnings("unused")
    public static <VALUE_TYPE> KeyedObjectKey.Basic<String, VALUE_TYPE> stringKey() {
        // noinspection unchecked
        return (KeyedObjectKey.Basic<String, VALUE_TYPE>) StringKey.INSTANCE;
    }

    /**
     * Generic String -> Object key representation, with support for null keys.
     * 
     * @param <VALUE_TYPE>
     */
    private static class NullSafeStringKey<VALUE_TYPE> extends KeyedObjectKey.NullSafeBasic<String, VALUE_TYPE> {

        private static KeyedObjectKey.NullSafeBasic<String, ?> INSTANCE = new NullSafeStringKey<>();

        @Override
        public String getKey(@NotNull final VALUE_TYPE value) {
            return value.toString();
        }
    }

    /**
     * Generic accessor for the singleton NullSafeStringKey instance.
     * 
     * @param <VALUE_TYPE> The type of the value for this key implementation
     * @return A String->StringKeyedObject key representation instance that supports null keys.
     */
    @SuppressWarnings("unused")
    public static <VALUE_TYPE> KeyedObjectKey.NullSafeBasic<String, VALUE_TYPE> nullSafeStringKey() {
        // noinspection unchecked
        return (KeyedObjectKey.NullSafeBasic<String, VALUE_TYPE>) NullSafeStringKey.INSTANCE;
    }

    /**
     * Interface to make auto-keying by toString() safer and more flexible.
     */
    public interface StringKeyedObject {

        default String getStringRepresentation() {
            return toString();
        }
    }

    /**
     * Generic String -> StringKeyedObject key representation.
     * 
     * @param <VALUE_TYPE>
     */
    private static class StringKeyedObjectKey<VALUE_TYPE extends StringKeyedObject>
            extends KeyedObjectKey.Basic<String, VALUE_TYPE> {

        private static KeyedObjectKey.Basic<String, ? extends StringKeyedObject> INSTANCE =
                new StringKeyedObjectKey<>();

        @Override
        public String getKey(@NotNull final VALUE_TYPE value) {
            return value.getStringRepresentation();
        }
    }

    /**
     * Generic accessor for the singleton StringKeyedObjectKey instance.
     * 
     * @param <VALUE_TYPE> The type of the value for this key implementation
     * @return A String->StringKeyedObject key representation instance.
     */
    @SuppressWarnings("unused")
    public static <VALUE_TYPE extends StringKeyedObject> KeyedObjectKey.Basic<String, VALUE_TYPE> stringKeyedObjectKey() {
        // noinspection unchecked
        return (KeyedObjectKey.Basic<String, VALUE_TYPE>) StringKeyedObjectKey.INSTANCE;
    }

    /**
     * Generic String -> StringKeyedObject key representation, with support for null keys.
     * 
     * @param <VALUE_TYPE>
     */
    private static class NullSafeStringKeyedObjectKey<VALUE_TYPE extends StringKeyedObject>
            extends KeyedObjectKey.NullSafeBasic<String, VALUE_TYPE> {

        private static KeyedObjectKey.NullSafeBasic<String, ? extends StringKeyedObject> INSTANCE =
                new NullSafeStringKeyedObjectKey<>();

        @Override
        public String getKey(@NotNull final VALUE_TYPE value) {
            return value.getStringRepresentation();
        }
    }

    /**
     * Generic accessor for the singleton NullSafeStringKeyedObjectKey instance.
     * 
     * @param <VALUE_TYPE> The type of the value for this key implementation
     * @return A String->StringKeyedObject key representation instance that supports null keys.
     */
    @SuppressWarnings("unused")
    public static <VALUE_TYPE extends StringKeyedObject> KeyedObjectKey.NullSafeBasic<String, VALUE_TYPE> nullSafeStringKeyedObjectKey() {
        // noinspection unchecked
        return (KeyedObjectKey.NullSafeBasic<String, VALUE_TYPE>) NullSafeStringKeyedObjectKey.INSTANCE;
    }

    /**
     * Generic CharSequence -> StringKeyedObject key representation.
     * 
     * @param <VALUE_TYPE>
     */
    private static class CharSequenceKey<VALUE_TYPE extends StringKeyedObject>
            implements KeyedObjectKey<CharSequence, VALUE_TYPE> {

        /**
         * Singleton CharSequenceKey instance.
         */
        private static KeyedObjectKey<CharSequence, ? extends StringKeyedObject> INSTANCE = new CharSequenceKey<>();


        @Override
        public CharSequence getKey(@NotNull final VALUE_TYPE value) {
            return value.getStringRepresentation();
        }

        @Override
        public int hashKey(@NotNull final CharSequence key) {
            if (key instanceof String || key instanceof StringCompatible) {
                return key.hashCode();
            }
            return CharSequenceUtils.hashCode(key);
        }

        @Override
        public boolean equalKey(@NotNull final CharSequence key, @NotNull final VALUE_TYPE value) {
            return CharSequenceUtils.contentEquals(key, getKey(value));
        }
    }

    /**
     * Generic accessor for the singleton CharSequenceKey instance.
     * 
     * @param <VALUE_TYPE> The type of the value for this key implementation
     * @return A CharSequence->StringKeyedObject key representation instance.
     */
    public static <VALUE_TYPE extends StringKeyedObject> KeyedObjectKey<CharSequence, VALUE_TYPE> charSequenceKey() {
        // noinspection unchecked
        return (KeyedObjectKey<CharSequence, VALUE_TYPE>) CharSequenceKey.INSTANCE;
    }

    /**
     * Generic CharSequence -> StringKeyedObject key representation.
     * 
     * @param <VALUE_TYPE>
     */
    private static class NullSafeCharSequenceKey<VALUE_TYPE extends StringKeyedObject>
            implements KeyedObjectKey<CharSequence, VALUE_TYPE> {

        /**
         * Singleton CharSequenceKey instance.
         */
        private static KeyedObjectKey<CharSequence, ? extends StringKeyedObject> INSTANCE =
                new NullSafeCharSequenceKey<>();


        @Override
        public CharSequence getKey(@NotNull final VALUE_TYPE value) {
            return value.getStringRepresentation();
        }

        @Override
        public int hashKey(final CharSequence key) {
            if (key instanceof String || key instanceof StringCompatible) {
                return key.hashCode();
            }
            return key == null ? 0 : CharSequenceUtils.hashCode(key);
        }

        @Override
        public boolean equalKey(final CharSequence key, @NotNull final VALUE_TYPE value) {
            return CharSequenceUtils.nullSafeContentEquals(key, getKey(value));
        }
    }

    /**
     * Generic accessor for the singleton CharSequenceKey instance, with support for null keys.
     * 
     * @param <VALUE_TYPE> The type of the value for this key implementation
     * @return A CharSequence->StringKeyedObject key representation instance.
     */
    @SuppressWarnings("unused")
    public static <VALUE_TYPE extends StringKeyedObject> KeyedObjectKey<CharSequence, VALUE_TYPE> nullSafeCharSequenceKey() {
        // noinspection unchecked
        return (KeyedObjectKey<CharSequence, VALUE_TYPE>) NullSafeCharSequenceKey.INSTANCE;
    }

    /**
     * Extracts a message from a throwable. If the message is null or empty, returns toString instead.
     *
     * @param t the throwable
     * @return the message or the string
     */
    public static String extractMessage(Throwable t) {
        return isNullOrEmpty(t.getMessage()) ? t.toString() : t.getMessage();
    }

    /**
     * Possibly replace an array element in one array with an array element in another array. The master strings are
     * searched to see if they contain an element that starts with the startsWithValue. If so, and the strings to be
     * replaced also contain an element which starts with startsWithValue, then that value is replaced with the master
     * string array's value. If multiple values match, only the first value for each array is processed.
     *
     * @param masterStrings the string array containing the values to use
     * @param toReplaceStrings the string array in which to perform the substitution
     * @param startsWithValue the starts-with value to search for
     */
    public static void replaceArrayValue(final String[] masterStrings,
            final String[] toReplaceStrings,
            final String startsWithValue) {
        Arrays.stream(masterStrings)
                .filter(s -> s.startsWith(startsWithValue))
                .findFirst()
                .ifPresent(s -> {
                    for (int i = 0; i < toReplaceStrings.length; i++) {
                        if (toReplaceStrings[i].startsWith(startsWithValue)) {
                            toReplaceStrings[i] = s;
                            return;
                        }
                    }
                });
    }
}
