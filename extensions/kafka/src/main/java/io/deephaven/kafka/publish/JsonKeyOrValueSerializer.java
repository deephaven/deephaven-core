/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.publish;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.base.clock.Clock;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
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

        makeFieldProcessors(columnNames, fieldNames);

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
                WritableObjectChunk<ObjectNode, Values> jsonChunk, RowSequence keys, boolean isRemoval);
    }

    abstract class JSONFieldProcessorImpl<ChunkType extends Chunk<Values>> extends JSONFieldProcessor {
        private final ColumnSource<?> chunkSource;

        public JSONFieldProcessorImpl(final String fieldName, final ColumnSource<?> chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        private class ContextImpl implements FieldContext {
            ChunkSource.GetContext getContext;

            ContextImpl(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(int size) {
            return new ContextImpl(size);
        }

        abstract void outputField(final int ii, final ObjectNode node, final ChunkType inputChunk);

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<ObjectNode, Values> jsonChunk,
                final RowSequence keys,
                final boolean previous) {
            final ContextImpl contextImpl = (ContextImpl) fieldContext;
            ChunkType inputChunk;
            if (previous) {
                inputChunk = (ChunkType) chunkSource.getPrevChunk(contextImpl.getContext, keys);
            } else {
                inputChunk = (ChunkType) chunkSource.getChunk(contextImpl.getContext, keys);
            }

            for (int ii = 0; ii < inputChunk.size(); ++ii) {
                final ObjectNode node = getChildNode(jsonChunk.get(ii));
                outputField(ii, node, inputChunk);
            }
        }
    }

    private JSONFieldProcessor makeByteFieldProcessor(final String fieldName, final ColumnSource<?> chunkSource) {
        return new JSONFieldProcessorImpl<ByteChunk<Values>>(fieldName, chunkSource) {
            @Override
            void outputField(final int ii, final ObjectNode node, final ByteChunk<Values> inputChunk) {
                final byte raw = inputChunk.get(ii);
                if (raw == QueryConstants.NULL_BYTE) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        };
    }

    private JSONFieldProcessor makeCharFieldProcessor(final String fieldName, final ColumnSource<?> chunkSource) {
        return new JSONFieldProcessorImpl<CharChunk<Values>>(fieldName, chunkSource) {
            @Override
            void outputField(final int ii, final ObjectNode node, final CharChunk<Values> inputChunk) {
                final char raw = inputChunk.get(ii);
                if (raw == QueryConstants.NULL_CHAR) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        };
    }

    private JSONFieldProcessor makeShortFieldProcessor(final String fieldName, final ColumnSource<?> chunkSource) {
        return new JSONFieldProcessorImpl<ShortChunk<Values>>(fieldName, chunkSource) {
            @Override
            void outputField(final int ii, final ObjectNode node, final ShortChunk<Values> inputChunk) {
                final short raw = inputChunk.get(ii);
                if (raw == QueryConstants.NULL_SHORT) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        };
    }

    private JSONFieldProcessor makeIntFieldProcessor(final String fieldName, final ColumnSource<?> chunkSource) {
        return new JSONFieldProcessorImpl<IntChunk<Values>>(fieldName, chunkSource) {
            @Override
            void outputField(final int ii, final ObjectNode node, final IntChunk<Values> inputChunk) {
                final int raw = inputChunk.get(ii);
                if (raw == QueryConstants.NULL_SHORT) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        };
    }

    private JSONFieldProcessor makeLongFieldProcessor(final String fieldName, final ColumnSource<?> chunkSource) {
        return new JSONFieldProcessorImpl<LongChunk<Values>>(fieldName, chunkSource) {
            @Override
            void outputField(final int ii, final ObjectNode node, final LongChunk<Values> inputChunk) {
                final long raw = inputChunk.get(ii);
                if (raw == QueryConstants.NULL_LONG) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        };
    }

    private JSONFieldProcessor makeFloatFieldProcessor(final String fieldName, final ColumnSource<?> chunkSource) {
        return new JSONFieldProcessorImpl<FloatChunk<Values>>(fieldName, chunkSource) {
            @Override
            void outputField(final int ii, final ObjectNode node, final FloatChunk<Values> inputChunk) {
                final float raw = inputChunk.get(ii);
                if (raw == QueryConstants.NULL_FLOAT) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        };
    }

    private JSONFieldProcessor makeDoubleFieldProcessor(final String fieldName, final ColumnSource<?> chunkSource) {
        return new JSONFieldProcessorImpl<DoubleChunk<Values>>(fieldName, chunkSource) {
            @Override
            void outputField(final int ii, final ObjectNode node, final DoubleChunk<Values> inputChunk) {
                final double raw = inputChunk.get(ii);
                if (raw == QueryConstants.NULL_DOUBLE) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    node.put(childNodeFieldName, raw);
                }
            }
        };
    }

    @FunctionalInterface
    interface PutFun<T> {
        void put(ObjectNode node, String childNodeFieldName, T raw);
    }

    private <T> JSONFieldProcessor makeObjectFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource,
            final PutFun<T> putFun) {
        return new JSONFieldProcessorImpl<ObjectChunk<T, Values>>(fieldName, chunkSource) {
            @Override
            void outputField(final int ii, final ObjectNode node, final ObjectChunk<T, Values> inputChunk) {
                final T raw = inputChunk.get(ii);
                if (raw == null) {
                    if (outputNulls) {
                        node.putNull(childNodeFieldName);
                    }
                } else {
                    putFun.put(node, childNodeFieldName, raw);
                }
            }
        };
    }

    private JSONFieldProcessor makeToStringFieldProcessor(final String fieldName, final ColumnSource<?> chunkSource) {
        return makeObjectFieldProcessor(
                fieldName,
                chunkSource,
                (final ObjectNode node,
                        final String childNodeFieldName,
                        final Object raw) -> node.put(childNodeFieldName, Objects.toString(raw)));
    }

    private JSONFieldProcessor makeBooleanFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeObjectFieldProcessor(
                fieldName,
                chunkSource,
                (PutFun<Boolean>) ObjectNode::put);
    }

    private JSONFieldProcessor makeBigIntegerFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeObjectFieldProcessor(
                fieldName,
                chunkSource,
                (PutFun<BigInteger>) ObjectNode::put);
    }

    private JSONFieldProcessor makeBigDecimalFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeObjectFieldProcessor(
                fieldName,
                chunkSource,
                (PutFun<BigDecimal>) ObjectNode::put);
    }

    private class TimestampFieldProcessor extends JSONFieldProcessor {
        public TimestampFieldProcessor(final String fieldName) {
            super(fieldName);
        }

        @Override
        FieldContext makeContext(final int size) {
            return null;
        }

        @Override
        public void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<ObjectNode, Values> jsonChunk,
                final RowSequence keys,
                final boolean isRemoval) {
            final String nanosString = String.valueOf(Clock.system().currentTimeNanos());
            for (int ii = 0; ii < jsonChunk.size(); ++ii) {
                getChildNode(jsonChunk.get(ii)).put(childNodeFieldName, nanosString);
            }
        }
    }

    void makeFieldProcessors(
            final String[] columnNames,
            final String[] fieldNames) {
        if (fieldNames != null && fieldNames.length != columnNames.length) {
            throw new IllegalArgumentException(
                    "fieldNames.length (" + fieldNames.length + ") != columnNames.length (" + columnNames.length + ")");
        }
        for (int i = 0; i < columnNames.length; ++i) {
            final String columnName = columnNames[i];
            try {
                if (fieldNames == null) {
                    makeFieldProcessor(columnName, columnName);
                } else {
                    makeFieldProcessor(columnName, fieldNames[i]);
                }
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Unknown column name " + columnName + " for table", e);
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
        final ColumnSource<?> src = source.getColumnSource(columnName);

        final Class<?> type = src.getType();
        if (type == byte.class) {
            fieldProcessors.add(makeByteFieldProcessor(fieldName, src));
        } else if (type == char.class) {
            fieldProcessors.add(makeCharFieldProcessor(fieldName, src));
        } else if (type == short.class) {
            fieldProcessors.add(makeShortFieldProcessor(fieldName, src));
        } else if (type == int.class) {
            fieldProcessors.add(makeIntFieldProcessor(fieldName, src));
        } else if (type == long.class) {
            fieldProcessors.add(makeLongFieldProcessor(fieldName, src));
        } else if (type == float.class) {
            fieldProcessors.add(makeFloatFieldProcessor(fieldName, src));
        } else if (type == double.class) {
            fieldProcessors.add(makeDoubleFieldProcessor(fieldName, src));
        } else if (type == Boolean.class) {
            fieldProcessors.add(makeBooleanFieldProcessor(fieldName, src));
        } else if (type == BigDecimal.class) {
            fieldProcessors.add(makeBigDecimalFieldProcessor(fieldName, src));
        } else if (type == BigInteger.class) {
            fieldProcessors.add(makeBigIntegerFieldProcessor(fieldName, src));
        } else {
            fieldProcessors.add(makeToStringFieldProcessor(fieldName, src));
        }
    }

    /**
     * Process the given update RowSequence and returns a list of JSON strings, reach representing one row of data.
     *
     * @param toProcess An RowSequence indicating which rows were involved
     * @param previous True if this should be performed using the 'previous' data instead of current, as for removals.
     * @return A List of Strings containing all of the parsed update statements
     */
    @Override
    public ObjectChunk<String, Values> handleChunk(Context context, RowSequence toProcess,
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

        private final WritableObjectChunk<String, Values> outputChunk;
        private final WritableObjectChunk<ObjectNode, Values> jsonChunk;
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
            SafeCloseable.closeAll(fieldContexts);
        }
    }
}
