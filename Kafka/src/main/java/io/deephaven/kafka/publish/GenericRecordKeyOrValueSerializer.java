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

    abstract static class GenericRecordFieldProcessor {
        final String fieldName;

        public GenericRecordFieldProcessor(final String fieldName) {
            this.fieldName = fieldName;
        }

        abstract FieldContext makeContext(int size);

        abstract void processField(FieldContext fieldContext,
                WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk, OrderedKeys keys, boolean isRemoval);
    }

    private static class ByteFieldProcessor extends GenericRecordFieldProcessor {
        private final ColumnSource chunkSource;

        public ByteFieldProcessor(final String fieldName, final ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class ByteContext implements FieldContext {
            ChunkSource.GetContext getContext;
            ByteChunk inputChunk;

            ByteContext(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new ByteContext(size);
        }

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                final OrderedKeys keys,
                final boolean previous) {
            final ByteContext byteContext = (ByteContext) fieldContext;
            if (previous) {
                byteContext.inputChunk = chunkSource.getPrevChunk(byteContext.getContext, keys).asByteChunk();
            } else {
                byteContext.inputChunk = chunkSource.getChunk(byteContext.getContext, keys).asByteChunk();
            }

            for (int ii = 0; ii < byteContext.inputChunk.size(); ++ii) {
                final byte raw = byteContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_BYTE) {
                    avroChunk.get(ii).put(fieldName, null);
                } else {
                    avroChunk.get(ii).put(fieldName, raw);
                }
            }
        }
    }

    private static class CharFieldProcessor extends GenericRecordFieldProcessor {
        private final ColumnSource chunkSource;

        public CharFieldProcessor(final String fieldName, final ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class CharContext implements FieldContext {
            ChunkSource.GetContext getContext;
            CharChunk inputChunk;

            CharContext(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new CharContext(size);
        }

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                final OrderedKeys keys,
                final boolean previous) {
            final CharContext charContext = (CharContext) fieldContext;
            if (previous) {
                charContext.inputChunk = chunkSource.getPrevChunk(charContext.getContext, keys).asCharChunk();
            } else {
                charContext.inputChunk = chunkSource.getChunk(charContext.getContext, keys).asCharChunk();
            }

            for (int ii = 0; ii < charContext.inputChunk.size(); ++ii) {
                final char raw = charContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_CHAR) {
                    avroChunk.get(ii).put(fieldName, null);
                } else {
                    avroChunk.get(ii).put(fieldName, raw);
                }
            }
        }
    }

    private static class ShortFieldProcessor extends GenericRecordFieldProcessor {
        private final ColumnSource chunkSource;

        public ShortFieldProcessor(final String fieldName, final ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class ShortContext implements FieldContext {
            ChunkSource.GetContext getContext;
            ShortChunk inputChunk;

            ShortContext(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new ShortContext(size);
        }

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                final OrderedKeys keys,
                final boolean previous) {
            final ShortContext shortContext = (ShortContext) fieldContext;
            if (previous) {
                shortContext.inputChunk = chunkSource.getPrevChunk(shortContext.getContext, keys).asShortChunk();
            } else {
                shortContext.inputChunk = chunkSource.getChunk(shortContext.getContext, keys).asShortChunk();
            }

            for (int ii = 0; ii < shortContext.inputChunk.size(); ++ii) {
                final short raw = shortContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_SHORT) {
                    avroChunk.get(ii).put(fieldName, null);
                } else {
                    avroChunk.get(ii).put(fieldName, raw);
                }
            }
        }
    }

    private static class IntFieldProcessor extends GenericRecordFieldProcessor {
        private final ColumnSource chunkSource;

        public IntFieldProcessor(final String fieldName, final ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class IntContext implements FieldContext {
            ChunkSource.GetContext getContext;
            IntChunk inputChunk;

            IntContext(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(final int size) {
            return new IntContext(size);
        }

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                final OrderedKeys keys,
                final boolean previous) {
            final IntContext intContext = (IntContext) fieldContext;
            if (previous) {
                intContext.inputChunk = chunkSource.getPrevChunk(intContext.getContext, keys).asIntChunk();
            } else {
                intContext.inputChunk = chunkSource.getChunk(intContext.getContext, keys).asIntChunk();
            }

            for (int ii = 0; ii < intContext.inputChunk.size(); ++ii) {
                final int raw = intContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_INT) {
                    avroChunk.get(ii).put(fieldName, null);
                } else {
                    avroChunk.get(ii).put(fieldName, raw);
                }
            }
        }
    }

    private static class LongFieldProcessor extends GenericRecordFieldProcessor {
        private final ColumnSource chunkSource;

        public LongFieldProcessor(final String fieldName, final ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class LongContext implements FieldContext {
            ChunkSource.GetContext getContext;
            LongChunk inputChunk;

            LongContext(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(final int size) {
            return new LongContext(size);
        }

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                final OrderedKeys keys,
                final boolean previous) {
            final LongContext longContext = (LongContext) fieldContext;
            if (previous) {
                longContext.inputChunk = chunkSource.getPrevChunk(longContext.getContext, keys).asLongChunk();
            } else {
                longContext.inputChunk = chunkSource.getChunk(longContext.getContext, keys).asLongChunk();
            }

            for (int ii = 0; ii < longContext.inputChunk.size(); ++ii) {
                final long raw = longContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_LONG) {
                    avroChunk.get(ii).put(fieldName, null);
                } else {
                    avroChunk.get(ii).put(fieldName, raw);
                }
            }
        }
    }

    private static class FloatFieldProcessor extends GenericRecordFieldProcessor {
        private final ColumnSource chunkSource;

        public FloatFieldProcessor(final String fieldName, final ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class FloatContext implements FieldContext {
            ChunkSource.GetContext getContext;
            FloatChunk inputChunk;

            FloatContext(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new FloatContext(size);
        }

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                final OrderedKeys keys,
                final boolean previous) {
            final FloatContext floatContext = (FloatContext) fieldContext;
            if (previous) {
                floatContext.inputChunk = chunkSource.getPrevChunk(floatContext.getContext, keys).asFloatChunk();
            } else {
                floatContext.inputChunk = chunkSource.getChunk(floatContext.getContext, keys).asFloatChunk();
            }

            for (int ii = 0; ii < floatContext.inputChunk.size(); ++ii) {
                final float raw = floatContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_FLOAT) {
                    avroChunk.get(ii).put(fieldName, null);
                } else {
                    avroChunk.get(ii).put(fieldName, raw);
                }
            }
        }
    }

    private static class DoubleFieldProcessor extends GenericRecordFieldProcessor {
        private final ColumnSource chunkSource;

        public DoubleFieldProcessor(final String fieldName, final ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class DoubleContext implements FieldContext {
            ChunkSource.GetContext getContext;
            DoubleChunk inputChunk;

            DoubleContext(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(final int size) {
            return new DoubleContext(size);
        }

        @Override
        void processField(FieldContext fieldContext, WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys, boolean previous) {
            final DoubleContext doubleContext = (DoubleContext) fieldContext;
            if (previous) {
                doubleContext.inputChunk = chunkSource.getPrevChunk(doubleContext.getContext, keys).asDoubleChunk();
            } else {
                doubleContext.inputChunk = chunkSource.getChunk(doubleContext.getContext, keys).asDoubleChunk();
            }

            for (int ii = 0; ii < doubleContext.inputChunk.size(); ++ii) {
                final double raw = doubleContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_DOUBLE) {
                    avroChunk.get(ii).put(fieldName, null);
                } else {
                    avroChunk.get(ii).put(fieldName, raw);
                }
            }
        }
    }

    private static class ObjectFieldProcessor<T> extends GenericRecordFieldProcessor {
        private final ColumnSource chunkSource;

        public ObjectFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class ObjectContext implements FieldContext {
            ChunkSource.GetContext getContext;
            ObjectChunk inputChunk;

            ObjectContext(int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new ObjectContext(size);
        }

        @Override
        void processField(FieldContext fieldContext, WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys, boolean previous) {
            final ObjectContext objectContext = (ObjectContext) fieldContext;
            if (previous) {
                objectContext.inputChunk = chunkSource.getPrevChunk(objectContext.getContext, keys).asObjectChunk();
            } else {
                objectContext.inputChunk = chunkSource.getChunk(objectContext.getContext, keys).asObjectChunk();
            }

            for (int ii = 0; ii < objectContext.inputChunk.size(); ++ii) {
                final Object raw = objectContext.inputChunk.get(ii);
                avroChunk.get(ii).put(fieldName, raw);
            }
        }
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
            fieldProcessors.add(new ByteFieldProcessor(fieldName, src));
        } else if (short.class.equals(type)) {
            fieldProcessors.add(new ShortFieldProcessor(fieldName, src));
        } else if (int.class.equals(type)) {
            fieldProcessors.add(new IntFieldProcessor(fieldName, src));
        } else if (double.class.equals(type)) {
            fieldProcessors.add(new DoubleFieldProcessor(fieldName, src));
        } else if (float.class.equals(type)) {
            fieldProcessors.add(new FloatFieldProcessor(fieldName, src));
        } else if (long.class.equals(type)) {
            fieldProcessors.add(new LongFieldProcessor(fieldName, src));
        } else if (char.class.equals(type)) {
            fieldProcessors.add(new CharFieldProcessor(fieldName, src));
        } else {
            fieldProcessors.add(new ObjectFieldProcessor<>(fieldName, src));
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
