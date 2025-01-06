//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.values.ValuesReader;

import java.lang.reflect.Array;

public class ObjectMaterializer<TYPE> extends ObjectMaterializerBase<TYPE> implements PageMaterializer {

    public static final class Factory<TYPE> implements PageMaterializerFactory {

        final ObjectCodec<TYPE> codec;
        final Class<TYPE> nativeType;

        public Factory(ObjectCodec<TYPE> codec, final Class<TYPE> nativeType) {
            this.codec = codec;
            this.nativeType = nativeType;
        }

        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            // noinspection unchecked
            return new ObjectMaterializer<>(dataReader, (TYPE) nullValue, numValues, codec, nativeType);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new ObjectMaterializer<>(dataReader, numValues, codec, nativeType);
        }
    };

    private final ValuesReader dataReader;
    private final ObjectCodec<TYPE> codec;

    private ObjectMaterializer(ValuesReader dataReader, int numValues, final ObjectCodec<TYPE> codec,
            final Class<TYPE> nativeType) {
        this(dataReader, null, numValues, codec, nativeType);
    }

    private ObjectMaterializer(ValuesReader dataReader, TYPE nullValue, int numValues, final ObjectCodec<TYPE> codec,
            final Class<TYPE> nativeType) {
        // noinspection unchecked
        super(nullValue, (TYPE[]) Array.newInstance(nativeType, numValues));
        this.dataReader = dataReader;
        this.codec = codec;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = codec.decode(dataReader.readBytes().toByteBuffer());
        }
    }
}
