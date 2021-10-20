package io.deephaven.kafka.publish;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class JsonKeyOrValueSerializer implements KeyOrValueSerializer<String> {
    /**
     * Our Json object to string converter
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * An empty JSON object that we use as the template for each output row (contains all the nested nodes).
     */
    private final ObjectNode emptyObjectNode;

    /**
     * The table we are reading from.
     */
    private final Table source;

    protected final String nestedObjectDelimiter;
    protected final boolean outputNulls;
    protected final List<JSONFieldProcessor> fieldProcessors = new ArrayList<>();

    public JsonKeyOrValueSerializer(final Table source,
                                    final String[] columnNames,
                                    final String[] fieldNames,
                                    final String timestampFieldName,
                                    final String nestedObjectDelimiter,
                                    final boolean outputNulls) {
        this.source = source;
        this.nestedObjectDelimiter = nestedObjectDelimiter;
        this.outputNulls = outputNulls;

        MultiFieldKeyOrValueSerializerUtils.makeFieldProcessors(columnNames, fieldNames, this::makeFieldProcessor);

        if (!StringUtils.isNullOrEmpty(timestampFieldName)) {
            fieldProcessors.add(new TimestampFieldProcessor(timestampFieldName));
        }

        this.emptyObjectNode = OBJECT_MAPPER.createObjectNode();

        // create any nested structure in our template
        if (nestedObjectDelimiter != null) {
            for (final JSONFieldProcessor fieldProcessor : this.fieldProcessors) {
                final String[] processorFieldNames = fieldProcessor.fieldName.split(nestedObjectDelimiter);
                ObjectNode node = emptyObjectNode;
                for (int i = 1; i < processorFieldNames.length; i++) {
                    ObjectNode child = (ObjectNode) node.get(processorFieldNames[i - 1]);
                    if (child == null) {
                        child = OBJECT_MAPPER.createObjectNode();
                        node.set(processorFieldNames[i - 1], child);
                    }
                    node = child;
                }
            }
        }
    }

    private interface FieldContext extends SafeCloseable {
    }

    abstract class JSONFieldProcessor {
        final String fieldName;
        protected final String[] fieldNames;
        protected final String childNodeFieldName;

        public JSONFieldProcessor(final String fieldName) {
            this.fieldName = fieldName;
            if (nestedObjectDelimiter != null) {
                this.fieldNames = fieldName.split(nestedObjectDelimiter);
                this.childNodeFieldName = fieldNames[fieldNames.length - 1];
            } else {
                this.fieldNames = new String[] {fieldName};
                childNodeFieldName = fieldName;
            }
        }

        protected ObjectNode getChildNode(final ObjectNode root) {
            ObjectNode child = root;
            for (int i = 0; i < fieldNames.length - 1; i++) {
                child = (ObjectNode) child.get(fieldNames[i]);
            }
            return child;
        }

        abstract FieldContext makeContext(int size);

        abstract void processField(FieldContext fieldContext,
                WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk, OrderedKeys keys, boolean isRemoval);
    }

    private class ByteFieldProcessor extends JSONFieldProcessor {
        private final ColumnSource chunkSource;

        public ByteFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class ByteContext implements FieldContext {
            ChunkSource.GetContext getContext;
            ByteChunk inputChunk;

            ByteContext(int size) {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final ByteContext byteContext = (ByteContext) fieldContext;
            if (previous) {
                byteContext.inputChunk = chunkSource.getPrevChunk(byteContext.getContext, keys).asByteChunk();
            } else {
                byteContext.inputChunk = chunkSource.getChunk(byteContext.getContext, keys).asByteChunk();
            }

            for (int ii = 0; ii < byteContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final byte raw = byteContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_BYTE) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        }
    }

    private class CharFieldProcessor extends JSONFieldProcessor {
        private final ColumnSource chunkSource;

        public CharFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class CharContext implements FieldContext {
            ChunkSource.GetContext getContext;
            CharChunk inputChunk;

            CharContext(int size) {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final CharFieldProcessor.CharContext charContext = (CharFieldProcessor.CharContext) fieldContext;
            if (previous) {
                charContext.inputChunk = chunkSource.getPrevChunk(charContext.getContext, keys).asCharChunk();
            } else {
                charContext.inputChunk = chunkSource.getChunk(charContext.getContext, keys).asCharChunk();
            }

            for (int ii = 0; ii < charContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final char raw = charContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_CHAR) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        }
    }

    private class ShortFieldProcessor extends JSONFieldProcessor {
        private final ColumnSource chunkSource;

        public ShortFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class ShortContext implements FieldContext {
            ChunkSource.GetContext getContext;
            ShortChunk inputChunk;

            ShortContext(int size) {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final ShortContext shortContext = (ShortContext) fieldContext;
            if (previous) {
                shortContext.inputChunk = chunkSource.getPrevChunk(shortContext.getContext, keys).asShortChunk();
            } else {
                shortContext.inputChunk = chunkSource.getChunk(shortContext.getContext, keys).asShortChunk();
            }

            for (int ii = 0; ii < shortContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final short raw = shortContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_SHORT) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        }
    }

    private class IntFieldProcessor extends JSONFieldProcessor {
        private final ColumnSource chunkSource;

        public IntFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class IntContext implements FieldContext {
            ChunkSource.GetContext getContext;
            IntChunk inputChunk;

            IntContext(int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new IntContext(size);
        }

        @Override
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final IntContext intContext = (IntContext) fieldContext;
            if (previous) {
                intContext.inputChunk = chunkSource.getPrevChunk(intContext.getContext, keys).asIntChunk();
            } else {
                intContext.inputChunk = chunkSource.getChunk(intContext.getContext, keys).asIntChunk();
            }

            for (int ii = 0; ii < intContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final int raw = intContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_INT) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        }
    }

    private class LongFieldProcessor extends JSONFieldProcessor {
        private final ColumnSource chunkSource;

        public LongFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class LongContext implements FieldContext {
            ChunkSource.GetContext getContext;
            LongChunk inputChunk;

            LongContext(int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new LongContext(size);
        }

        @Override
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final LongContext longContext = (LongContext) fieldContext;
            if (previous) {
                longContext.inputChunk = chunkSource.getPrevChunk(longContext.getContext, keys).asLongChunk();
            } else {
                longContext.inputChunk = chunkSource.getChunk(longContext.getContext, keys).asLongChunk();
            }

            for (int ii = 0; ii < longContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final long raw = longContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_LONG) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        }
    }

    private class FloatFieldProcessor extends JSONFieldProcessor {
        private final ColumnSource chunkSource;

        public FloatFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class FloatContext implements FieldContext {
            ChunkSource.GetContext getContext;
            FloatChunk inputChunk;

            FloatContext(int size) {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final FloatContext floatContext = (FloatContext) fieldContext;
            if (previous) {
                floatContext.inputChunk = chunkSource.getPrevChunk(floatContext.getContext, keys).asFloatChunk();
            } else {
                floatContext.inputChunk = chunkSource.getChunk(floatContext.getContext, keys).asFloatChunk();
            }

            for (int ii = 0; ii < floatContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final float raw = floatContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_FLOAT) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        }
    }

    private class DoubleFieldProcessor extends JSONFieldProcessor {
        private final ColumnSource chunkSource;

        public DoubleFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class DoubleContext implements FieldContext {
            ChunkSource.GetContext getContext;
            DoubleChunk inputChunk;

            DoubleContext(int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new DoubleContext(size);
        }

        @Override
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final DoubleContext doubleContext = (DoubleContext) fieldContext;
            if (previous) {
                doubleContext.inputChunk = chunkSource.getPrevChunk(doubleContext.getContext, keys).asDoubleChunk();
            } else {
                doubleContext.inputChunk = chunkSource.getChunk(doubleContext.getContext, keys).asDoubleChunk();
            }

            for (int ii = 0; ii < doubleContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final double raw = doubleContext.inputChunk.get(ii);
                if (raw == QueryConstants.NULL_DOUBLE) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        }
    }

    private class ToStringFieldProcessor extends JSONFieldProcessor {
        private final ColumnSource chunkSource;

        public ToStringFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class ToStringContext implements FieldContext {
            ChunkSource.GetContext getContext;
            ObjectChunk inputChunk;

            ToStringContext(int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new ToStringContext(size);
        }

        @Override
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final ToStringContext toStringContext = (ToStringContext) fieldContext;
            if (previous) {
                toStringContext.inputChunk = chunkSource.getPrevChunk(toStringContext.getContext, keys).asObjectChunk();
            } else {
                toStringContext.inputChunk = chunkSource.getChunk(toStringContext.getContext, keys).asObjectChunk();
            }

            for (int ii = 0; ii < toStringContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final Object raw = toStringContext.inputChunk.get(ii);
                if (raw == null) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, Objects.toString(raw));
                }
            }
        }
    }

    private abstract class ObjectFieldProcessor<T> extends JSONFieldProcessor {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk,
                OrderedKeys keys, boolean previous) {
            final ObjectContext objectContext = (ObjectContext) fieldContext;
            if (previous) {
                objectContext.inputChunk = chunkSource.getPrevChunk(objectContext.getContext, keys).asObjectChunk();
            } else {
                objectContext.inputChunk = chunkSource.getChunk(objectContext.getContext, keys).asObjectChunk();
            }

            for (int ii = 0; ii < objectContext.inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                final T raw = (T) objectContext.inputChunk.get(ii);
                if (raw == null) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    putValue(node, raw);
                }
            }
        }

        abstract void putValue(ObjectNode node, T value);
    }

    private class BooleanFieldProcessor extends ObjectFieldProcessor<Boolean> {
        public BooleanFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName, chunkSource);
        }

        @Override
        void putValue(ObjectNode node, Boolean value) {
            node.put(childNodeFieldName, value);
        }
    }

    private class BigIntegerFieldProcessor extends ObjectFieldProcessor<BigInteger> {
        public BigIntegerFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName, chunkSource);
        }

        @Override
        void putValue(ObjectNode node, BigInteger value) {
            node.put(childNodeFieldName, value);
        }
    }

    private class BigDecimalFieldProcessor extends ObjectFieldProcessor<BigDecimal> {
        public BigDecimalFieldProcessor(String fieldName, ColumnSource chunkSource) {
            super(fieldName, chunkSource);
        }

        @Override
        void putValue(ObjectNode node, BigDecimal value) {
            node.put(childNodeFieldName, value);
        }
    }

    private class TimestampFieldProcessor extends JSONFieldProcessor {
        public TimestampFieldProcessor(String fieldName) {
            super(fieldName);
        }

        @Override
        FieldContext makeContext(int size) {
            return null;
        }

        @Override
        public void processField(FieldContext fieldContext,
                WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk, OrderedKeys keys, boolean isRemoval) {
            final String nanosString = String.valueOf(DBDateTime.now().getNanos());
            for (int ii = 0; ii < jsonChunk.size(); ++ii) {
                getChildNode(jsonChunk.get(ii)).put(childNodeFieldName, nanosString);
            }
        }
    }

    /**
     * Create a field processor that translates a given column from its Deephaven row number to output of the intended
     * type.
     *
     *
     * @param columnName The Deephaven column to be translated into publishable format
     * @param fieldName The name of the field in the output (if needed).
     */
    private void makeFieldProcessor(final String columnName, final String fieldName) {
        // getColumn should throw a ColumnNotFoundException if it can't find the column, which will blow us up here.
        @SuppressWarnings("rawtypes")
        final ColumnSource src = source.getColumnSource(columnName);

        if (byte.class.equals(src.getType())) {
            fieldProcessors.add(new ByteFieldProcessor(fieldName, src));
        } else if (short.class.equals(src.getType())) {
            fieldProcessors.add(new ShortFieldProcessor(fieldName, src));
        } else if (int.class.equals(src.getType())) {
            fieldProcessors.add(new IntFieldProcessor(fieldName, src));
        } else if (double.class.equals(src.getType())) {
            fieldProcessors.add(new DoubleFieldProcessor(fieldName, src));
        } else if (float.class.equals(src.getType())) {
            fieldProcessors.add(new FloatFieldProcessor(fieldName, src));
        } else if (long.class.equals(src.getType())) {
            fieldProcessors.add(new LongFieldProcessor(fieldName, src));
        } else if (char.class.equals(src.getType())) {
            fieldProcessors.add(new CharFieldProcessor(fieldName, src));
        } else if (Boolean.class.equals(src.getType())) {
            fieldProcessors.add(new BooleanFieldProcessor(fieldName, src));
        } else if (BigDecimal.class.equals(src.getType())) {
            fieldProcessors.add(new BigDecimalFieldProcessor(fieldName, src));
        } else if (BigInteger.class.equals(src.getType())) {
            fieldProcessors.add(new BigIntegerFieldProcessor(fieldName, src));
        } else {
            fieldProcessors.add(new ToStringFieldProcessor(fieldName, src));
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
    public ObjectChunk<String, Attributes.Values> handleChunk(Context context, OrderedKeys toProcess,
            boolean previous) {
        final JsonContext jsonContext = (JsonContext) context;

        jsonContext.outputChunk.setSize(0);
        jsonContext.jsonChunk.setSize(toProcess.intSize());
        for (int position = 0; position < toProcess.intSize(); ++position) {
            jsonContext.jsonChunk.set(position, emptyObjectNode.deepCopy());
        }


        for (int ii = 0; ii < fieldProcessors.size(); ++ii) {
            fieldProcessors.get(ii).processField(jsonContext.fieldContexts[ii], jsonContext.jsonChunk, toProcess,
                    previous);
        }

        for (int position = 0; position < toProcess.intSize(); ++position) {
            try {
                jsonContext.outputChunk.add(OBJECT_MAPPER.writeValueAsString(jsonContext.jsonChunk.get(position)));
            } catch (JsonProcessingException e) {
                throw new KafkaPublisherException("Failed to write JSON message", e);
            }
        }

        return jsonContext.outputChunk;
    }

    @Override
    public Context makeContext(int size) {
        return new JsonContext(size);
    }

    private final class JsonContext implements Context {

        private final WritableObjectChunk<String, Attributes.Values> outputChunk;
        private final WritableObjectChunk<ObjectNode, Attributes.Values> jsonChunk;
        private final FieldContext[] fieldContexts;

        public JsonContext(int size) {
            this.outputChunk = WritableObjectChunk.makeWritableChunk(size);
            this.jsonChunk = WritableObjectChunk.makeWritableChunk(size);
            this.fieldContexts = new FieldContext[fieldProcessors.size()];
            for (int ii = 0; ii < fieldProcessors.size(); ++ii) {
                fieldContexts[ii] = fieldProcessors.get(ii).makeContext(size);
            }
        }

        @Override
        public void close() {
            outputChunk.close();
            jsonChunk.close();
            SafeCloseable.closeArray(fieldContexts);
        }
    }
}
