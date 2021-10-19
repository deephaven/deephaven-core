package io.deephaven.kafka.publish;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class GenericRecordKeyOrValueSerializer implements KeyOrValueSerializer<GenericRecord> {
    /**
     * The table we are reading from.
     */
    private final Table source;

    /**
     * The Avro schema.
     */
    private final Schema schema;

    private interface FieldContext extends SafeCloseable {
    }

    abstract class GenericRecordFieldProcessor {
        final String fieldName;

        public GenericRecordFieldProcessor(final String fieldName) {
            this.fieldName = fieldName;
        }

        abstract FieldContext makeContext(int size);

        abstract void processField(FieldContext fieldContext,
                WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk, OrderedKeys keys, boolean isRemoval);
    }

    private class ByteFieldProcessor extends GenericRecordFieldProcessor {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys, boolean previous) {
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

    private class CharFieldProcessor extends GenericRecordFieldProcessor {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys, boolean previous) {
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

    private class ShortFieldProcessor extends GenericRecordFieldProcessor {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys, boolean previous) {
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

    private class IntFieldProcessor extends GenericRecordFieldProcessor {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys, boolean previous) {
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

    private class LongFieldProcessor extends GenericRecordFieldProcessor {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys, boolean previous) {
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

    private class FloatFieldProcessor extends GenericRecordFieldProcessor {
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
        void processField(FieldContext fieldContext, WritableObjectChunk<GenericRecord, Attributes.Values> avroChunk,
                OrderedKeys keys, boolean previous) {
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

    private class DoubleFieldProcessor extends GenericRecordFieldProcessor {
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

    private class ObjectFieldProcessor<T> extends GenericRecordFieldProcessor {
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

    private class TimestampFieldProcessor extends GenericRecordFieldProcessor {
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

    protected final List<GenericRecordFieldProcessor> fieldProcessors = new ArrayList<>();

    public GenericRecordKeyOrValueSerializer(final Table source,
            final Schema schema,
            final Map<String, String> columnsToOutputFields,
            final Set<String> excludedColumns,
            final boolean autoValueMapping,
            final boolean ignoreMissingColumns,
            final String timestampFieldName) {
        this.source = source;
        this.schema = schema;

        final String[] columnNames = source.getDefinition().getColumnNamesArray();
        final List<String> missingColumns = new ArrayList<>();

        // Create all the auto-mapped columns
        for (final String columnName : columnNames) {
            if (excludedColumns.contains(columnName) || columnsToOutputFields.containsValue(columnName)) {
                continue;
            }
            if (autoValueMapping) {
                makeFieldProcessor(columnName, columnName);
            } else if (!ignoreMissingColumns) {
                missingColumns.add(columnName);
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
            fieldProcessors.add(new TimestampFieldProcessor(timestampFieldName));
        }
    }

    /**
     * Create a field processor that translates a given column from its Deephaven row number to output of the intended
     * type.
     *
     * Override this method in descendant classes to change the output style.
     *
     * @param fieldName The name of the field in the output (if needed).
     * @param columnName The Deephaven column to be translated into publishable format
     */
    protected void makeFieldProcessor(final String fieldName, final String columnName) {
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

    /**
     * Create a builder for processing Deephaven table data into string output
     */
    public static class Builder<SERIALIZED_TYPE> implements KeyOrValueSerializer.Factory<SERIALIZED_TYPE> {

        private final Map<String, String> columnToField = new LinkedHashMap<>();
        private final Set<String> excludedColumns = new HashSet<>();
        private Schema schema = null;
        private boolean autoValueMapping = true;
        private boolean ignoreMissingColumns = false;
        private String timestampFieldName = null;

        public Builder<SERIALIZED_TYPE> schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder<SERIALIZED_TYPE> schema(File schema) throws IOException {
            this.schema = new Schema.Parser().parse(schema);
            return this;
        }

        public Builder<SERIALIZED_TYPE> schema(String schema) {
            this.schema = new Schema.Parser().parse(schema);
            return this;
        }

        /**
         * Enables or disables automatic value mapping (true by default).
         *
         * If auto value mapping is enabled, any column that was not defined [either by excludeColumn or mapColumn] is
         * automatically mapped to a JSON field of the same name.
         *
         * @param autoValueMapping should automatic value mapping be enabled
         *
         * @return this builder
         */
        @ScriptApi
        public Builder<SERIALIZED_TYPE> autoValueMapping(final boolean autoValueMapping) {
            this.autoValueMapping = autoValueMapping;
            return this;
        }

        /**
         * Permit the builder to silently ignore any columns not specified. False by default. If auto value mapping is
         * enabled, this has no effect.
         *
         * @param ignoreMissingColumns True if the builder should ignore table columns with no specified behavior. false
         *        if the builder should throw an exception if columns are found with no mapping.
         * @return this builder
         */
        @ScriptApi
        public Builder<SERIALIZED_TYPE> ignoreMissingColumns(final boolean ignoreMissingColumns) {
            this.ignoreMissingColumns = ignoreMissingColumns;
            return this;
        }

        /**
         * Indicates that a column is unmapped, and therefore not included in the output. You may not exclude a column
         * that has already been excluded or mapped.
         *
         * @param column name of the column in the output table
         *
         * @return this builder
         */
        @ScriptApi
        public Builder<SERIALIZED_TYPE> excludeColumn(@NotNull final String column) {
            checkColumnAlreadyExcluded(column);
            checkColumnAlreadyMapped(column);
            excludedColumns.add(column);
            return this;
        }

        /**
         * Map a Deephaven column to an output field of the same name. You may map the same column to multiple output
         * fields, but may only map a given field once.
         *
         * @param column The name of the Deephaven column to export
         * @return this builder
         */
        @ScriptApi
        public Builder<SERIALIZED_TYPE> mapColumn(@NotNull final String column) {
            return mapColumn(column, column);
        }

        /**
         * Map a Deephaven column to a specified output field. You may map multiple output fields from a single column.,
         * but may only map a given field once.
         *
         * @param column The name of the Deephaven column to export
         * @param field The name of the field to produce
         * @return this builder
         */
        @ScriptApi
        public Builder<SERIALIZED_TYPE> mapColumn(@NotNull final String column, @NotNull final String field) {
            checkColumnAlreadyExcluded(column);
            checkFieldAlreadyMapped(field);
            columnToField.put(field, column);
            return this;
        }

        /**
         * Include a timestamp indicating when this data was processed.
         *
         * @param timestampFieldName The name of the field to show a timestamp, or null for no timestamp.
         * @return this builder
         */
        @ScriptApi
        public Builder<SERIALIZED_TYPE> timestampFieldName(final String timestampFieldName) {
            if (timestampFieldName != null) {
                checkFieldAlreadyMapped(timestampFieldName);
            }
            this.timestampFieldName = timestampFieldName;
            return this;
        }

        public void validateColumns(@NotNull final TableDefinition tableDefinition) {
            if (!autoValueMapping && !ignoreMissingColumns) {
                final List<String> missingColumns = tableDefinition.getColumnStream()
                        .map(ColumnDefinition::getName)
                        .filter(cn -> !columnToField.containsKey(cn) && !excludedColumns.contains(cn))
                        .collect(Collectors.toList());
                if (!missingColumns.isEmpty()) {
                    throw new IllegalArgumentException("Incompatible table definition: found columns without mappings "
                            + missingColumns);
                }
            }
            final List<String> unavailableColumns = columnToField.keySet().stream()
                    .filter(cn -> tableDefinition.getColumn(cn) == null)
                    .collect(Collectors.toList());
            if (!unavailableColumns.isEmpty()) {
                throw new IllegalArgumentException("Incompatible table definition: unavailable mapped columns "
                        + unavailableColumns);
            }
        }

        @Override
        public List<String> sourceColumnNames(@NotNull final TableDefinition tableDefinition) {
            return Collections.unmodifiableList(tableDefinition.getColumnStream()
                    .map(ColumnDefinition::getName)
                    .filter(cn -> columnToField.containsKey(cn) || (autoValueMapping && !excludedColumns.contains(cn)))
                    .collect(Collectors.toList()));
        }

        @Override
        public KeyOrValueSerializer<SERIALIZED_TYPE> create(@NotNull final Table source) {
            if (schema == null) {
                throw new KafkaPublisherException("Schema is required for a GenericRecordKeyOrValueSerializer");
            }
            // noinspection unchecked
            return (KeyOrValueSerializer<SERIALIZED_TYPE>) new GenericRecordKeyOrValueSerializer(source, schema,
                    columnToField, excludedColumns, autoValueMapping, ignoreMissingColumns,
                    timestampFieldName);
        }

        private void checkColumnAlreadyExcluded(@NotNull final String column) {
            if (excludedColumns.contains(column)) {
                throw new KafkaPublisherException("Column " + column + " is already excluded.");
            }
        }

        private void checkColumnAlreadyMapped(@NotNull final String column) {
            if (columnToField.containsValue(column)) {
                throw new KafkaPublisherException("Column " + column + " is already mapped.");
            }
        }

        private void checkFieldAlreadyMapped(@NotNull final String field) {
            if (columnToField.containsKey(field) || field.equals(timestampFieldName)) {
                throw new KafkaPublisherException("Field " + field + " is already mapped.");
            }
        }
    }
}
