/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.regex.Pattern;

public class GenericRecordObjectFieldCopier implements FieldCopier {
    private final String[] fieldPath;

    public GenericRecordObjectFieldCopier(final String fieldPathStr, final Pattern separator) {
        this.fieldPath = GenericRecordUtil.getFieldPath(fieldPathStr, separator);
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk,
            final WritableChunk<Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableObjectChunk<Object, Values> output = publisherChunk.asWritableObjectChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord record = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Object value = GenericRecordUtil.getPath(record, fieldPath);
            output.set(ii + destOffset, value);
        }
    }
}
