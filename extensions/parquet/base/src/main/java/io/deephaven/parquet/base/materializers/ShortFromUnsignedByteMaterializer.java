//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

public class ShortFromUnsignedByteMaterializer extends ShortMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new ShortFromUnsignedByteMaterializer(dataReader, (short) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new ShortFromUnsignedByteMaterializer(dataReader, numValues);
        }
    };

    public static short convertValue(int value) {
        return (short) Byte.toUnsignedInt((byte) value);
    }

    private final PageValueReader dataReader;

    private ShortFromUnsignedByteMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, (short) 0, numValues);
    }

    private ShortFromUnsignedByteMaterializer(PageValueReader dataReader, short nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readInteger());
        }
    }
}
