//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;

/**
 * Materializer for String data on PLAIN-encoded BINARY pages, decoding in bulk straight from the page buffer.
 *
 * @see StringMaterializer
 */
public class PlainBinaryStringMaterializer extends ObjectMaterializerBase<String> implements PageMaterializer {

    private final PlainBinaryStringValuesReader dataReader;

    PlainBinaryStringMaterializer(PlainBinaryStringValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    PlainBinaryStringMaterializer(PlainBinaryStringValuesReader dataReader, String nullValue, int numValues) {
        super(nullValue, new String[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        dataReader.readStrings(data, startIndex, endIndex);
    }
}
