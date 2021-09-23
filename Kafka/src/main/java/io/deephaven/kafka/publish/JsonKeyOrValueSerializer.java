package io.deephaven.kafka.publish;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class JsonKeyOrValueSerializer implements KeyOrValueSerializer {
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
    final DynamicTable source;


    protected interface ProcessFunction {
        void accept(final ObjectNode node, final String field, final long idx, final boolean isRemoval);
    }

    protected class JSONFieldProcessor {
        protected final String[] fieldNames;
        protected final String childNodeFieldName;
        protected final ProcessFunction processFunction;

        public JSONFieldProcessor(final String fieldName, final ProcessFunction processFunction) {
            this.processFunction = processFunction;
            if(nestedObjectDelimiter != null) {
                this.fieldNames = fieldName.split(nestedObjectDelimiter);
                this.childNodeFieldName = fieldNames[fieldNames.length-1];
            } else {
                this.fieldNames = new String[] { fieldName };
                childNodeFieldName = fieldName;
            }
        }

        protected ObjectNode getChildNode(final ObjectNode root) {
            ObjectNode child = root;
            for(int i = 0; i < fieldNames.length-1; i++) {
                child = (ObjectNode)child.get(fieldNames[i]);
            }
            return child;
        }

        public void accept(final ObjectNode rootNode, final long idx, final boolean isRemoval) {
            // get the node this field belongs in (in case of nesting), pass that along with the final field name
            processFunction.accept(getChildNode(rootNode), childNodeFieldName, idx, isRemoval);
        }
    }

    protected final String nestedObjectDelimiter;
    protected final boolean outputNulls;
    protected final Map<String, JSONFieldProcessor> fieldProcessors = new HashMap<>();

    public JsonKeyOrValueSerializer(final DynamicTable source,
                                    final Map<String, String> columnsToOutputFields,
                                    final Set<String> excludedColumns,
                                    final boolean autoValueMapping,
                                    final boolean ignoreMissingColumns,
                                    final String timestampFieldName,
                                    final String nestedObjectDelimiter,
                                    final boolean outputNulls) {
        this.source = source;
        this.nestedObjectDelimiter = nestedObjectDelimiter;
        this.outputNulls = outputNulls;

        final String[] columnNames = source.getDefinition().getColumnNamesArray();
        final List<String> missingColumns = new ArrayList<>();

        // Create all the auto-mapped columns
        for (final String columnName : columnNames) {
            if (excludedColumns.contains(columnName) || columnsToOutputFields.containsValue(columnName)) {
                continue;
            }
            if (autoValueMapping) {
                makeFieldProcessor(columnName, columnName);
            } else {
                if (!ignoreMissingColumns) {
                    missingColumns.add(columnName);
                }
            }
        }

        if (!missingColumns.isEmpty()) {
            final StringBuilder sb = new StringBuilder("Found columns without mappings " + missingColumns);
            if (!excludedColumns.isEmpty()) {
                sb.append(", unmapped=").append(excludedColumns);
            }
            if (!columnsToOutputFields.isEmpty()) {
                sb.append(", mapped to fields=").append(columnsToOutputFields.keySet());
            }
            throw new KafkaPublisherException(sb.toString());
        }

        // Now create all the processors for specifically-named fields
        columnsToOutputFields.forEach(this::makeFieldProcessor);

        if (!StringUtils.isNullOrEmpty(timestampFieldName)) {
            fieldProcessors.put(timestampFieldName, makeTimestampProcessor(timestampFieldName));
        }

        this.emptyObjectNode = OBJECT_MAPPER.createObjectNode();

        // create any nested structure in our template
        if(nestedObjectDelimiter != null) {
            for(final String fieldName: this.fieldProcessors.keySet()) {
                final String[] fieldNames = fieldName.split(nestedObjectDelimiter);
                ObjectNode node = emptyObjectNode;
                for (int i = 1; i < fieldNames.length; i++) {
                    ObjectNode child = (ObjectNode) node.get(fieldNames[i-1]);
                    if (child == null) {
                        child = OBJECT_MAPPER.createObjectNode();
                        node.set(fieldNames[i-1], child);
                    }
                    node = child;
                }
            }
        }
    }

    // Utility method to set an ObjectNode value, that respects the outputNulls option
    private <T> void putNullable(final ObjectNode node, final String field, final T value, final BiConsumer<String,T> putter) {
        if (value == null) {
            if (outputNulls) {
                node.putNull(field);
            }
        } else {
            putter.accept(field, value);
        }
    }

    /**
     * Create a field processor that translates a given column from its Deephaven row number to output of the
     * intended type.
     *
     * Override this method in descendant classes to change the output style.
     *
     * @param fieldName The name of the field in the output (if needed).
     * @param columnName The Deephaven column to be translated into publishable format
     */
    protected void makeFieldProcessor(final String fieldName, final String columnName) {
        // getColumn should throw a ColumnNotFoundException if it can't find the column, which will blow us up here.
        @SuppressWarnings("rawtypes") final ColumnSource src = source.getColumnSource(columnName);

        if (byte.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) -> {
                final byte raw = isRemoval ? src.getPrevByte(idx) : src.getByte(idx);
                if (raw == QueryConstants.NULL_BYTE) {
                    if (outputNulls) {
                        node.putNull(field);
                    }
                } else {
                    node.put(field, raw);
                }
            }));
        } else if (short.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) -> {
                final short raw = isRemoval ? src.getPrevShort(idx) : src.getShort(idx);
                if (raw == QueryConstants.NULL_SHORT) {
                    if (outputNulls) {
                        node.putNull(field);
                    }
                } else {
                    node.put(field, raw);
                }
            }));
        } else if (int.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) -> {
                final int raw = isRemoval ? src.getPrevInt(idx) : src.getInt(idx);
                if (raw == QueryConstants.NULL_INT) {
                    if (outputNulls) {
                        node.putNull(field);
                    }
                } else {
                    node.put(field, raw);
                }
            }));
        } else if (double.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) -> {
                final double raw = isRemoval ? src.getPrevDouble(idx) : src.getDouble(idx);
                if (raw == QueryConstants.NULL_DOUBLE) {
                    if (outputNulls) {
                        node.putNull(field);
                    }
                } else {
                    node.put(field, raw);
                }
            }));
        } else if (float.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) -> {
                final float raw = isRemoval ? src.getPrevFloat(idx) : src.getFloat(idx);
                if (raw == QueryConstants.NULL_FLOAT) {
                    if (outputNulls) {
                        node.putNull(field);
                    }
                } else {
                    node.put(field, raw);
                }
            }));
        } else if (long.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) -> {
                final long raw = isRemoval ? src.getPrevLong(idx) : src.getLong(idx);
                if (raw == QueryConstants.NULL_LONG) {
                    if (outputNulls) {
                        node.putNull(field);
                    }
                } else {
                    node.put(field, raw);
                }
            }));
        } else if (char.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) -> {
                final char raw = isRemoval ? src.getPrevChar(idx) : src.getChar(idx);
                if (raw == QueryConstants.NULL_CHAR) {
                    if (outputNulls) {
                        node.putNull(field);
                    }
                } else {
                    node.put(field, raw);
                }
            }));
        } else if (Boolean.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) ->
                    putNullable(node, field, isRemoval ? src.getPrevBoolean(idx) : src.getBoolean(idx), node::put)));
        } else if (BigDecimal.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) ->
                    putNullable(node, field, isRemoval ? (BigDecimal) src.getPrev(idx) : (BigDecimal) src.get(idx), node::put)));
        } else if (BigInteger.class.equals(src.getType())) {
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) ->
                    putNullable(node, field, isRemoval ? (BigInteger) src.getPrev(idx) : (BigInteger) src.get(idx), node::put)));
        } else {
            // all other types, convert to string
            fieldProcessors.put(fieldName, new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) -> {
                final Object returnValue = isRemoval ? src.getPrev(idx) : src.get(idx);
                final String str = returnValue == null ? null : returnValue.toString();
                putNullable(node, field, str, node::put);
            }));
        }
    }

    /**
     * Create a field processor that outputs the time when this function was called.
     *
     * @param fieldName The name of the output field to populate
     * @return a function that produces a timestamp.
     */
    private JSONFieldProcessor makeTimestampProcessor(final String fieldName) {
        return new JSONFieldProcessor(fieldName, (node, field, idx, isRemoval) ->
                node.put(field, String.valueOf(DBDateTime.now().getNanos())));
    }

    /**
     * Process the given update index and returns a list of JSON strings, reach representing one row of data.
     *
     * @param toProcess An Index indicating which rows were involved
     * @param previous True if this should be performed using the 'previous' data instead of current, as for removals.
     * @return A List of Strings containing all of the parsed update statements
     */
    @Override
    public ObjectChunk<String, Attributes.Values> handleChunk(Context context, OrderedKeys toProcess, boolean previous) {
        final JsonContext jsonContext = (JsonContext) context;
        jsonContext.outputChunk.setSize(0);

        toProcess.forAllLongs((rowKey) -> {
            // create each output object as a deep copy that has any nested structure we need
            final ObjectNode json = emptyObjectNode.deepCopy();

            // build the output
            fieldProcessors.forEach((key,value) -> ((JSONFieldProcessor)value).accept(json, rowKey, previous));

            // add the converted object to the list
            try {
                jsonContext.outputChunk.add(OBJECT_MAPPER.writeValueAsString(json));
            } catch (JsonProcessingException e) {
                throw new KafkaPublisherException("Failed to write JSON message", e);
            }
        });

        return jsonContext.outputChunk;
    }

    @Override
    public Context makeContext(int size) {
        return new JsonContext(size);
    }

    private static final class JsonContext implements Context {
        final WritableObjectChunk<String, Attributes.Values> outputChunk;

        public JsonContext(int size) {
            this.outputChunk = WritableObjectChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            outputChunk.close();
        }
    }

    /**
     * Create a builder for processing Deephaven table data into string output
     */
    public static class Builder {
        final Map<String, String> columnToTextField = new HashMap<>();
        final Set<String> excludedColumns = new HashSet<>();
        boolean autoValueMapping = true;
        boolean ignoreMissingColumns = false;
        String timestampFieldName = null;
        String nestedObjectDelimiter = null;
        boolean outputNulls = true;

        /**
         * Enables or disables automatic value mapping (true by default).
         *
         * If auto value mapping is enabled, any column that was not defined [either by excludeColumn
         * or mapColumn] is automatically mapped to a JSON field of the same name.
         *
         * @param autoValueMapping should automatic value mapping be enabled
         *
         * @return this builder
         */
        @ScriptApi
        public Builder autoValueMapping(final boolean autoValueMapping) {
            this.autoValueMapping = autoValueMapping;
            return this;
        }

        /**
         * Permit the builder to silently ignore any columns not specified. False by default.
         * If auto value mapping is enabled, this has no effect.
         *
         * @param ignoreMissingColumns True if the builder should ignore table columns with no specified behavior.
         *                             false if the builder should throw an exception if columns are found with no
         *                             mapping.
         * @return this builder
         */
        @ScriptApi
        public Builder ignoreMissingColumns(final boolean ignoreMissingColumns) {
            this.ignoreMissingColumns = ignoreMissingColumns;
            return this;
        }

        /**
         * Indicates that a column is unmapped, and therefore not included in the output.
         * You may not exclude a column that has already been excluded or mapped.
         *
         * @param column name of the column in the output table
         *
         * @return this builder
         */
        @ScriptApi
        public Builder excludeColumn(@NotNull final String column) {
            checkColumnAlreadyExcluded(column);
            checkColumnAlreadyMapped(column);
            excludedColumns.add(column);
            return this;
        }

        /**
         * Map a Deephaven column to an output field of the same name.
         * You may map the same column to multiple output fields, but may only map a given field once.
         *
         * @param column The name of the Deephaven column to export
         * @return this builder
         */
        @ScriptApi
        public Builder mapColumn(@NotNull final String column) {
            return mapColumn(column, column);
        }

        /**
         * Map a Deephaven column to a specified output field.
         * You may map multiple output fields from a single column., but may only map a given field once.
         *
         * @param column The name of the Deephaven column to export
         * @param field The name of the field to produce
         * @return this builder
         */
        @ScriptApi
        public Builder mapColumn(@NotNull final String column, @NotNull final String field) {
            checkColumnAlreadyExcluded(column);
            checkFieldAlreadyMapped(field);
            columnToTextField.put(field, column);
            return this;
        }

        /**
         * Include a timestamp indicating when this data was processed.
         *
         * @param timestampFieldName The name of the field to show a timestamp, or null for no timestamp.
         * @return this builder
         */
        @ScriptApi
        public Builder timestampFieldName(final String timestampFieldName) {
            if (timestampFieldName != null) {
                checkFieldAlreadyMapped(timestampFieldName);
            }
            this.timestampFieldName = timestampFieldName;
            return this;
        }

        /**
         * The delimiter used to generate nested output objects from column names.
         *
         * @param nestedObjectDelimiter the delimiter string/character.
         * @return this builder
         */
        @ScriptApi
        public Builder nestedObjectDelimiter(final String nestedObjectDelimiter) {
            this.nestedObjectDelimiter = nestedObjectDelimiter;
            return this;
        }

        /**
         * Whether to output null values.
         *
         * @param outputNulls to output nulls
         * @return this builder
         */
        @ScriptApi
        public Builder outputNulls(final boolean outputNulls) {
            this.outputNulls = outputNulls;
            return this;
        }

        /**
         * When implemented in a subordinate class, create the actual factory for this adapter.
         * @return A factory for objects of this type.
         */
        public Function<DynamicTable, ? extends JsonKeyOrValueSerializer> buildFactory() {
            return (tbl) -> new JsonKeyOrValueSerializer(tbl,
                    columnToTextField, excludedColumns, autoValueMapping, ignoreMissingColumns,
                    timestampFieldName, nestedObjectDelimiter, outputNulls);
        }

        private void checkColumnAlreadyExcluded(@NotNull final String column) {
            if (excludedColumns.contains(column)) {
                throw new KafkaPublisherException("Column " + column + " is already excluded.");
            }
        }

        private void checkColumnAlreadyMapped(@NotNull final String column) {
            if (columnToTextField.containsValue(column)) {
                throw new KafkaPublisherException("Column " + column + " is already mapped.");
            }
        }

        private void checkFieldAlreadyMapped(@NotNull final String field) {
            if (columnToTextField.containsKey(field) || field.equals(timestampFieldName)) {
                throw new KafkaPublisherException("Field " + field + " is already mapped.");
            }
        }
    }
}
