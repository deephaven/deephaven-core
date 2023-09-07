/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.function.Function;
import java.util.function.Predicate;

@Immutable
@BuildableStyle
public abstract class FieldOptions {

    private static final FieldOptions DEFAULTS = builder().build();
    private static final FieldOptions EXCLUDE = builder().include(false).build();

    public static Builder builder() {
        return ImmutableFieldOptions.builder();
    }

    /**
     * The defaults options. Equivalent to {@code builder().build()}.
     *
     * @return the default options
     */
    public static FieldOptions defaults() {
        return DEFAULTS;
    }

    /**
     * Creates the options to exclude a field. Equivalent to {@code builder().include(false).build()}.
     *
     * @return the options to exclude a field
     */
    public static FieldOptions exclude() {
        return EXCLUDE;
    }

    /**
     * Creates a field options function that maps the {@code include} predicate to {@link FieldOptions#include()}.
     * Equivalent to {@code fp -> builder().include(include.test(fp)).build()}.
     * 
     * @param include the include function
     * @return the field path functions
     */
    @SuppressWarnings("unused")
    public static Function<FieldPath, FieldOptions> includeIf(Predicate<FieldPath> include) {
        return fp -> builder().include(include.test(fp)).build();
    }

    /**
     * The behavior when handling a protobuf {@link com.google.protobuf.Descriptors.FieldDescriptor.Type#MESSAGE
     * message} type.
     */
    public interface WellKnownBehavior {
        /**
         * Parse the field as a well-known type if known, otherwise parse recursively.
         */
        static WellKnownBehavior asWellKnown() {
            return WellKnownImpl.AS_WELL_KNOWN;
        }

        /**
         * Parse the field recursively.
         */
        static WellKnownBehavior asRecursive() {
            return WellKnownImpl.AS_RECURSIVE;
        }
    }

    /**
     * The behavior when handling a protobuf {@link com.google.protobuf.Descriptors.FieldDescriptor.Type#BYTES bytes}
     * field.
     */
    public interface BytesBehavior {

        /**
         * Parsers the field as a {@code byte[]}.
         */
        static BytesBehavior asByteArray() {
            return BytesImpl.AS_BYTE_ARRAY;
        }

        /**
         * Parses the field as a {@link com.google.protobuf.ByteString ByteString}.
         */
        static BytesBehavior asByteString() {
            return BytesImpl.AS_BYTES_STRING;
        }
    }

    /**
     * The behavior when handling a protobuf {@code map} field.
     */
    public interface MapBehavior {
        /**
         * Parses the field as a {@link java.util.Map Map}.
         */
        static MapBehavior asMap() {
            return MapsImpl.AS_MAP;
        }

        /**
         * Parses the field as a {@code repeated MapFieldEntry}.
         */
        static MapBehavior asRepeated() {
            return MapsImpl.AS_REPEATED;
        }
    }

    /**
     * If the field should be included for parsing. By default, is {@code true}.
     *
     * @return if the field should be included
     */
    @Default
    public boolean include() {
        return true;
    }

    /**
     * The well-known message behavior. By default, is {@link WellKnownBehavior#asWellKnown()}.
     *
     * @return the well-known message behavior
     */
    @Default
    public WellKnownBehavior wellKnown() {
        return WellKnownBehavior.asWellKnown();
    }

    /**
     * The {@code bytes} type behavior. By default, is {@link BytesBehavior#asByteArray()}.
     *
     * @return the bytes field behavior
     */
    @Default
    public BytesBehavior bytes() {
        return BytesBehavior.asByteArray();
    }

    /**
     * The {@code map} type behavior. By default, is {@link MapBehavior#asMap()}.
     *
     * @return the map field behavior.
     */
    @Default
    public MapBehavior map() {
        return MapBehavior.asMap();
    }

    public interface Builder {
        Builder include(boolean include);

        Builder wellKnown(WellKnownBehavior wellKnown);

        Builder bytes(BytesBehavior bytes);

        Builder map(MapBehavior map);

        FieldOptions build();
    }

    enum WellKnownImpl implements WellKnownBehavior {
        AS_WELL_KNOWN, AS_RECURSIVE;
    }

    enum BytesImpl implements BytesBehavior {
        AS_BYTE_ARRAY, AS_BYTES_STRING;
    }

    enum MapsImpl implements MapBehavior {
        AS_MAP, AS_REPEATED;
    }
}
