/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.string.EncodingInfo;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.base.string.cache.StringCache;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.SourceTableComponentFactory;
import io.deephaven.db.v2.sources.regioned.decoder.EncodedStringDecoder;
import io.deephaven.db.v2.sources.regioned.decoder.SimpleStringDecoder;
import io.deephaven.util.codec.ObjectDecoder;

/**
 * Factory interface for regioned source table components.
 */
public interface RegionedTableComponentFactory extends SourceTableComponentFactory {

    <DATA_TYPE> RegionedColumnSource<DATA_TYPE> createRegionedColumnSource(ColumnDefinition<DATA_TYPE> columnDefinition);

    static <DATA_TYPE> ObjectDecoder<DATA_TYPE> getStringDecoder(Class<DATA_TYPE> dataType, ColumnDefinition columnDefinition) {
        final EncodingInfo encodingInfo = columnDefinition.getEncodingInfo();
        //noinspection unchecked
        return (encodingInfo.isSimple() || CompressedString.class.isAssignableFrom(dataType)) ?
                new SimpleStringDecoder(dataType) : new EncodedStringDecoder(dataType, encodingInfo);

    }

    static <STRING_LIKE_TYPE extends CharSequence> ObjectDecoder<STRING_LIKE_TYPE> getStringDecoder(StringCache<STRING_LIKE_TYPE> cache, ColumnDefinition columnDefinition) {
        final EncodingInfo encodingInfo = columnDefinition.getEncodingInfo();
        return encodingInfo.isSimple() || CompressedString.class.isAssignableFrom(cache.getType()) ?
                new SimpleStringDecoder<>(cache) : new EncodedStringDecoder<>(cache, encodingInfo);

    }
}
