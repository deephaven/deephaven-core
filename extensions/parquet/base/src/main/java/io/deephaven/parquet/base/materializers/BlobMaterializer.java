//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

/**
 * Materializer for binary data.
 */
public class BlobMaterializer extends ObjectMaterializerBase<Binary> implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BlobMaterializer(dataReader, (Binary) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BlobMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private BlobMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private BlobMaterializer(ValuesReader dataReader, Binary nullValue, int numValues) {
        super(nullValue, new Binary[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = dataReader.readBytes();
        }
    }
}
