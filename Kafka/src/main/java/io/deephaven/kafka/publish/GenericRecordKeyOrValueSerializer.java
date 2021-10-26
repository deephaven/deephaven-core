package io.deephaven.kafka.publish;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.*;

public class GenericRecordKeyOrValueSerializer implements KeyOrValueSerializer<GenericRecord> {
    /**
     * The table we are reading from.
     */
    private final Table source;

    /**
     * The Avro schema.
     */
    private final Schema schema;


    protected final List<GenericRecordFieldProcessor> fieldProcessors = new ArrayList<>();

    public GenericRecordKeyOrValueSerializer(final Table source,
            final Schema schema,
            final String[] columnNames,
            final String[] fieldNames,
            final String timestampFieldName) {
        this.source = source;
        this.schema = schema;

        MultiFieldKeyOrValueSerializerUtils.makeFieldProcessors(columnNames, fieldNames, this::makeFieldProcessor);

        if (!StringUtils.isNullOrEmpty(timestampFieldName)) {
            fieldProcessors.add(new TimestampFieldProcessor(timestampFieldName));
        }
    }

    private interface FieldContext extends SafeCloseable {
    }

    private abstract static class GenericRecordFieldProcessor {
        final String fieldName;

        public GenericRecordFieldProcessor(final String fieldName) {
            this.fieldName = fieldName;
        }

        abstract FieldContext makeContext(int size);

        abstract void processField(
                FieldContext fieldContext,
                WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys,
                boolean isRemoval);
    }

    private abstract static class GenericRecordFieldProcessorImpl<ChunkType extends Chunk<Attributes.Values>>
            extends GenericRecordFieldProcessor {
        private final ColumnSource<?> chunkSource;

        public GenericRecordFieldProcessorImpl(final String fieldName, final ColumnSource<?> chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        class ContextImpl implements FieldContext {
            ChunkSource.GetContext getContext;
            ChunkType inputChunk;

            ContextImpl(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(final int size) {
            return new ContextImpl(size);
        }

        abstract Object getFieldElement(int i, ContextImpl contextImpl);

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                final OrderedKeys keys,
                final boolean previous) {
            final ContextImpl contextImpl = (ContextImpl) fieldContext;
            if (previous) {
                contextImpl.inputChunk = (ChunkType) chunkSource.getPrevChunk(contextImpl.getContext, keys);
            } else {
                contextImpl.inputChunk = (ChunkType) chunkSource.getChunk(contextImpl.getContext, keys);
            }

            for (int ii = 0; ii < contextImpl.inputChunk.size(); ++ii) {
                avroChunk.get(ii).put(fieldName, getFieldElement(ii, contextImpl));
            }
        }
    }

    private static GenericRecordFieldProcessor makeByteFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return new GenericRecordFieldProcessorImpl<ByteChunk<Attributes.Values>>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ContextImpl contextImpl) {
                return QueryConstants.asObjectOrNull(contextImpl.inputChunk.get(ii));
            }
        };
    }

    private static GenericRecordFieldProcessor makeCharFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return new GenericRecordFieldProcessorImpl<CharChunk<Attributes.Values>>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ContextImpl contextImpl) {
                return QueryConstants.asObjectOrNull(contextImpl.inputChunk.get(ii));
            }
        };
    }

    private static GenericRecordFieldProcessor makeShortFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return new GenericRecordFieldProcessorImpl<ShortChunk<Attributes.Values>>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ContextImpl contextImpl) {
                return QueryConstants.asObjectOrNull(contextImpl.inputChunk.get(ii));
            }
        };
    }

    private static GenericRecordFieldProcessor makeIntFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return new GenericRecordFieldProcessorImpl<IntChunk<Attributes.Values>>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ContextImpl contextImpl) {
                return QueryConstants.asObjectOrNull(contextImpl.inputChunk.get(ii));
            }
        };
    }

    private static GenericRecordFieldProcessor makeLongFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return new GenericRecordFieldProcessorImpl<LongChunk<Attributes.Values>>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ContextImpl contextImpl) {
                return QueryConstants.asObjectOrNull(contextImpl.inputChunk.get(ii));
            }
        };
    }

    private static GenericRecordFieldProcessor makeFloatFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return new GenericRecordFieldProcessorImpl<FloatChunk<Attributes.Values>>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ContextImpl contextImpl) {
                return QueryConstants.asObjectOrNull(contextImpl.inputChunk.get(ii));
            }
        };
    }

    private static GenericRecordFieldProcessor makeDoubleFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return new GenericRecordFieldProcessorImpl<DoubleChunk<Attributes.Values>>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ContextImpl contextImpl) {
                return QueryConstants.asObjectOrNull(contextImpl.inputChunk.get(ii));
            }
        };
    }

    private static GenericRecordFieldProcessor makeObjectFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return new GenericRecordFieldProcessorImpl<ObjectChunk<?, Attributes.Values>>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ContextImpl contextImpl) {
                return contextImpl.inputChunk.get(ii);
            }
        };
    }

    private static class TimestampFieldProcessor extends GenericRecordFieldProcessor {
        public TimestampFieldProcessor(String fieldName) {
            super(fieldName);
        }

        @Override
        FieldContext makeContext(int size) {
            return null;
        }

        @Override
        public void processField(FieldContext fieldContext,
                WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk, OrderedKeys keys, boolean isRemoval) {
            // do we really want nanos instead of micros; there are avro things for micros/millis?
            final long nanos = DBDateTime.now().getNanos();
            for (int ii = 0; ii < avroChunk.size(); ++ii) {
                avroChunk.get(ii).put(fieldName, nanos);
            }
        }
    }

    /**
     * Create a field processor that translates a given column from its Deephaven row number to output of the intended
     * type.
     *
     * @param columnName The Deephaven column to be translated into publishable format
     * @param fieldName The name of the field in the output (if needed).
     */
    private void makeFieldProcessor(final String columnName, final String fieldName) {
        // getColumn should throw a ColumnNotFoundException if it can't find the column, which will blow us up here.
        @SuppressWarnings("rawtypes")
        final ColumnSource src = source.getColumnSource(columnName);
        final Class<?> type = src.getType();
        if (byte.class.equals(type)) {
            fieldProcessors.add(makeByteFieldProcessor(fieldName, src));
        } else if (short.class.equals(type)) {
            fieldProcessors.add(makeShortFieldProcessor(fieldName, src));
        } else if (int.class.equals(type)) {
            fieldProcessors.add(makeIntFieldProcessor(fieldName, src));
        } else if (double.class.equals(type)) {
            fieldProcessors.add(makeDoubleFieldProcessor(fieldName, src));
        } else if (float.class.equals(type)) {
            fieldProcessors.add(makeFloatFieldProcessor(fieldName, src));
        } else if (long.class.equals(type)) {
            fieldProcessors.add(makeLongFieldProcessor(fieldName, src));
        } else if (char.class.equals(type)) {
            fieldProcessors.add(makeCharFieldProcessor(fieldName, src));
        } else {
            fieldProcessors.add(makeObjectFieldProcessor(fieldName, src));
        }
    }

    /**
     * Process the given update index and returns a list of JSON strings, reach representing one row of data.
     *
     * @param toProcess An Index indicating which rows were involved
     * @param previous True if this should be performed using the 'previous' data instead of current, as for removals.
     * @return A List of Strings containing all of the parsed update statements
     */
    @Override
    public ObjectChunk<GenericRecord, Attributes.Values> handleChunk(Context context, OrderedKeys toProcess,
            boolean previous) {
        final AvroContext avroContext = (AvroContext) context;

        avroContext.avroChunk.setSize(toProcess.intSize());
        for (int position = 0; position < toProcess.intSize(); ++position) {
            avroContext.avroChunk.set(position, new GenericData.Record(schema));
        }

        for (int ii = 0; ii < fieldProcessors.size(); ++ii) {
            fieldProcessors.get(ii).processField(avroContext.fieldContexts[ii], avroContext.avroChunk, toProcess,
                    previous);
        }

        return avroContext.avroChunk;
    }

    @Override
    public Context makeContext(int size) {
        return new AvroContext(size);
    }

    private final class AvroContext implements Context {

        private final WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk;
        private final FieldContext[] fieldContexts;

        public AvroContext(int size) {
            this.avroChunk = WritableObjectChunk.makeWritableChunk(size);
            this.fieldContexts = new FieldContext[fieldProcessors.size()];
            for (int ii = 0; ii < fieldProcessors.size(); ++ii) {
                fieldContexts[ii] = fieldProcessors.get(ii).makeContext(size);
            }
        }

        @Override
        public void close() {
            avroChunk.close();
            SafeCloseable.closeArray(fieldContexts);
        }
    }
}
