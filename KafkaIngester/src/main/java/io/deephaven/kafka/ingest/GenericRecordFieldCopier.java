package io.deephaven.kafka.ingest;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.chunk.*;

/**
 * A field copier that handles generic records.
 */
public interface GenericRecordFieldCopier extends FieldCopier {
    /**
     * Create a field copier from a GenericRecord to the given chunk and data type
     *
     * @param fieldName the name of the field in the generic record
     * @param chunkType the type of chunk in the publisher
     * @param dataType  the dataType in the output definition
     * @return a GenericRecordField copier for the given field and destination type
     */
    static GenericRecordFieldCopier make(String fieldName, ChunkType chunkType, Class<?> dataType) {
        switch (chunkType) {
            case Char:
                return new CharFieldCopier(fieldName);
            case Byte:
                return new ByteFieldCopier(fieldName);
            case Short:
                return new ShortFieldCopier(fieldName);
            case Int:
                return new IntFieldCopier(fieldName);
            case Long:
                if (dataType == DBDateTime.class) {
                    throw new UnsupportedOperationException();
                }
                return new LongFieldCopier(fieldName);
            case Float:
                return new FloatFieldCopier(fieldName);
            case Double:
                return new DoubleFieldCopier(fieldName);
            case Object:
                if (dataType == String.class) {
                    return new StringFieldCopier(fieldName);
                } else {
                    return new ObjectFieldCopier(fieldName);
                }
        }
        throw new IllegalArgumentException("Can not convert field of type " + dataType);
    }
}
