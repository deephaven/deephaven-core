/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.string.cache.StringCache;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.ColumnToCodecMappings;
import io.deephaven.db.v2.SourceTableComponentFactory;
import io.deephaven.db.v2.sources.regioned.decoder.SimpleStringDecoder;
import io.deephaven.util.codec.ObjectDecoder;

/**
 * Factory interface for regioned source table components.
 */
public interface RegionedTableComponentFactory extends SourceTableComponentFactory {

    <DATA_TYPE> RegionedColumnSource<DATA_TYPE> createRegionedColumnSource(
            ColumnDefinition<DATA_TYPE> columnDefinition,
            ColumnToCodecMappings codecMappings);

    static <DATA_TYPE> ObjectDecoder<DATA_TYPE> getStringDecoder(
            final Class<DATA_TYPE> dataType,
            final ColumnDefinition<?> columnDefinition) {
        return new SimpleStringDecoder(dataType);

    }

    static <STRING_LIKE_TYPE extends CharSequence> ObjectDecoder<STRING_LIKE_TYPE> getStringDecoder(
            final StringCache<STRING_LIKE_TYPE> cache,
            final ColumnDefinition<?> columnDefinition) {
        return new SimpleStringDecoder<>(cache);

    }
}
