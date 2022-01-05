package io.deephaven.kafka.ingest;

import org.apache.avro.Schema;

import java.util.regex.Pattern;

public abstract class GenericRecordFieldCopier implements FieldCopier {
    protected final int[] fieldPath;
    protected GenericRecordFieldCopier(final String fieldPathStr, final Pattern separator, final Schema schema) {
        this.fieldPath = GenericRecordUtil.getFieldPath(fieldPathStr, separator, schema);
    }
}
