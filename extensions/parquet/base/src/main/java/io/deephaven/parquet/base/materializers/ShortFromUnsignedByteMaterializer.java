//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

public class ShortFromUnsignedByteMaterializer extends ShortMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new ShortFromUnsignedByteMaterializer(dataReader, (short) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new ShortFromUnsignedByteMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private ShortFromUnsignedByteMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, (short) 0, numValues);
    }

    private ShortFromUnsignedByteMaterializer(ValuesReader dataReader, short nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = (short) Byte.toUnsignedInt((byte) dataReader.readInteger());
        }
    }
}
