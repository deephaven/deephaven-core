//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.configuration.Configuration;
import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

public class StringMaterializer extends ObjectMaterializerBase<String> implements PageMaterializer {

    /**
     * Escape hatch: set to {@code false} to fall back to parquet's {@code BinaryPlainValuesReader}. Read per page
     * rather than cached, so it can be flipped in a running JVM without a restart.
     */
    public static final String ALLOW_PLAIN_BINARY_STRING_DECODER_PROP = "deephaven.parquet.plainBinaryStringDecoder";

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return dataReader instanceof PlainBinaryStringValuesReader
                    ? new PlainBinaryStringMaterializer(
                            (PlainBinaryStringValuesReader) dataReader, (String) nullValue, numValues)
                    : new StringMaterializer(dataReader, (String) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return dataReader instanceof PlainBinaryStringValuesReader
                    ? new PlainBinaryStringMaterializer((PlainBinaryStringValuesReader) dataReader, numValues)
                    : new StringMaterializer(dataReader, numValues);
        }

        @Override
        public boolean allowPlainBinaryStringDecoder() {
            return Configuration.getInstance()
                    .getBooleanWithDefault(ALLOW_PLAIN_BINARY_STRING_DECODER_PROP, true);
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
