/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit GenericRecordCharFieldCopier and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.regex.Pattern;

public class GenericRecordShortFieldCopier extends GenericRecordFieldCopier {
    public GenericRecordShortFieldCopier(final String fieldPathStr, final Pattern separator, final Schema schema) {
        super(fieldPathStr, separator, schema);
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk,
            final WritableChunk<Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableShortChunk<Values> output = publisherChunk.asWritableShortChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord record = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Short value = (Short) GenericRecordUtil.getPath(record, fieldPath);
            output.set(ii + destOffset, TypeUtils.unbox(value));
        }
    }
}
