/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.apache.avro.Schema;

import java.util.regex.Pattern;

public class GenericRecordGenericContainerFieldCopier extends GenericRecordFieldCopier {

    public GenericRecordGenericContainerFieldCopier(final String fieldPathStr, final Pattern separator, final Schema schema) {
        super(fieldPathStr, separator, schema);
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
            output.set(ii + destOffset, inputChunk.get(ii + sourceOffset));
        }
    }
}
