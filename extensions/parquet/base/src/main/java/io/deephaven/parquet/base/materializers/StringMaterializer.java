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
     * Enables {@link PlainBinaryStringValuesReader}. Read per page rather than cached, so it can be flipped in a
     * running JVM to A/B the two decoders without a restart.
     */
    public static final String USE_PLAIN_BINARY_STRING_DECODER_PROP = "deephaven.parquet.plainBinaryStringDecoder";

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new StringMaterializer(dataReader, (String) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new StringMaterializer(dataReader, numValues);
        }

        @Override
        public boolean usePlainBinaryStringDecoder() {
            return Configuration.getInstance()
                    .getBooleanWithDefault(USE_PLAIN_BINARY_STRING_DECODER_PROP, false);
        }
    };

    private final ValuesReader dataReader;
    /** Non-null when {@link #dataReader} supports bulk decoding; resolved once to keep {@link #fillValues} simple. */
    private final PlainBinaryStringValuesReader bulkReader;

    private StringMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private StringMaterializer(ValuesReader dataReader, String nullValue, int numValues) {
        super(nullValue, new String[numValues]);
        this.dataReader = dataReader;
        this.bulkReader = dataReader instanceof PlainBinaryStringValuesReader
                ? (PlainBinaryStringValuesReader) dataReader
                : null;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        if (bulkReader != null) {
            bulkReader.readStrings(data, startIndex, endIndex);
            return;
        }
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = dataReader.readBytes().toStringUsingUTF8();
        }
    }
}
