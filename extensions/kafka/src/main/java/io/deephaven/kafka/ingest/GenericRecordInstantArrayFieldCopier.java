//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.time.DateTimeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.time.Instant;
import java.util.regex.Pattern;

public class GenericRecordInstantArrayFieldCopier extends GenericRecordFieldCopier {
    private final long multiplier;

    public GenericRecordInstantArrayFieldCopier(final String fieldPathStr, final Pattern separator, final Schema schema,
            final long multiplier) {
        super(fieldPathStr, separator, schema);
        this.multiplier = multiplier;
    }

    private static Instant[] convertArray(final GenericArray<?> ga, final long multiplier) {
        final int gaSize = ga.size();
        if (gaSize == 0) {
            return DateTimeUtils.ZERO_LENGTH_INSTANT_ARRAY;
        }
        final Instant[] out = new Instant[ga.size()];
        int i = 0;
        for (Object o : ga) {
            out[i] = DateTimeUtils.epochNanosToInstant(multiplier * (long) o);
            ++i;
        }
        return out;
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
            final GenericArray<?> genericArray = (GenericArray<?>) GenericRecordUtil.getPath(record, fieldPath);
            if (genericArray == null) {
                output.set(ii + destOffset, null);
                continue;
            }
            output.set(ii + destOffset, convertArray(genericArray, multiplier));
        }
    }
}
