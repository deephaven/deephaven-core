//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PlainBinaryPageReaderFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.nio.ByteBuffer;

public class StringMaterializer extends ObjectMaterializerBase<String> implements PageMaterializer {

    public static final PlainBinaryPageReaderFactory FACTORY = new PlainBinaryPageReaderFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return dataReader instanceof PlainBinaryStringValuesReader plainBinaryReader
                    ? new PlainBinaryStringMaterializer(plainBinaryReader, (String) nullValue, numValues)
                    : new StringMaterializer(dataReader, (String) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return dataReader instanceof PlainBinaryStringValuesReader plainBinaryReader
                    ? new PlainBinaryStringMaterializer(plainBinaryReader, numValues)
                    : new StringMaterializer(dataReader, numValues);
        }

        @Override
        public ValuesReader makePlainBinaryValuesReader(final ByteBuffer in) {
            return new PlainBinaryStringValuesReader(in);
        }
    };

    private final ValuesReader dataReader;

    private StringMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private StringMaterializer(ValuesReader dataReader, String nullValue, int numValues) {
        super(nullValue, new String[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = dataReader.readBytes().toStringUsingUTF8();
        }
    }
}
