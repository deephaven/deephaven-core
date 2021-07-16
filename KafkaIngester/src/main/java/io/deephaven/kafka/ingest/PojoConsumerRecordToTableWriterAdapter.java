
package io.deephaven.kafka.ingest;

import io.deephaven.compilertools.CompilerTools;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.text.Indenter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.commons.lang3.ClassUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Converts a consumer record containing Java objects to a Deephaven row.
 *
 * The constructor will use reflection combined with the builder parameters to automatically map fields or methods in
 * the key and value classes to columns in the output table and compile an appropriate adapter class.  Each record is
 * then passed to the adapter class, requiring no per-record reflection.
 *
 * <p>This adapter is intended for use with custom deserializers or with using Avro with compiled
 * org.apache.avro.specific.SpecificRecord objects that contain a field for each schema element.</p>
 *
 * <p></p>After processing explicit mappings for fields; the value class is searched for exact matches followed by the key
 * class using the following precedence:
 * <ol>
 *     <li>a public method with a the same name as the column and no arguments</li>
 *     <li>a public method beginning with "get" followed by the name of the column, and no arguments</li>
 *     <li>a public field with the same name as the column, and no arguments</li>
 * </ol>
 * If caseInsensitiveSearch is specified on the builder, the same precedence is followed for the remaining columns;
 * but using case insensitive matching of method names.</p>
 *
 * @param <K> the type of the key record
 * @param <V> the type of the value record
 *
 */
@SuppressWarnings("unused")
public class PojoConsumerRecordToTableWriterAdapter<K, V> implements ConsumerRecordToTableWriterAdapter {
    private final TableWriter writer;
    private final RowSetter<Integer> kafkaPartitionColumnSetter;
    private final RowSetter<Long> offsetColumnSetter;
    private final RowSetter<DBDateTime> timestampColumnSetter;
    private final ConsumerRecordToTableWriterAdapter compiledPojoAdapter;

    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private PojoConsumerRecordToTableWriterAdapter(TableWriter<?> writer, String kafkaPartitionColumnName, String offsetColumnName, String timestampColumnName,
                                                   Class<K> keyClass, Class<V> valueClass,
                                                   boolean allowUnmapped, boolean caseInsensitiveSearch, boolean printClassBody, Set<String> columnsUnmapped,
                                                   Map<String, String> columnToKeyField, Map<String, String> columnToValueField,
                                                   Map<String, String> columnToKeyMethod, Map<String, String> columnToValueMethod,
                                                   Map<String, String> columnToSetter) {
        final String [] columnNames = writer.getColumnNames();
        final Class [] columnTypes = writer.getColumnTypes();

        final Map<String, Class> columnNameToType = new LinkedHashMap<>();
        for (int ii = 0; ii < columnNames.length; ++ii) {
            columnNameToType.put(columnNames[ii], columnTypes[ii]);
        }

        this.writer = writer;
        if (offsetColumnName != null) {
            offsetColumnSetter = writer.getSetter(offsetColumnName, Long.class);
            columnNameToType.remove(offsetColumnName);
        } else {
            offsetColumnSetter = null;
        }
        if (kafkaPartitionColumnName != null) {
            kafkaPartitionColumnSetter = writer.getSetter(kafkaPartitionColumnName, Integer.class);
            columnNameToType.remove(kafkaPartitionColumnName);
        } else {
            kafkaPartitionColumnSetter = null;
        }
        if (timestampColumnName != null) {
            timestampColumnSetter = writer.getSetter(timestampColumnName, DBDateTime.class);
            columnNameToType.remove(timestampColumnName);
        } else {
            timestampColumnSetter = null;
        }

        final List<String> assignments = new ArrayList<>();

        columnToSetter.forEach((col, setter) -> {
            final Class<?> colType = columnNameToType.remove(col);
            if (colType == null) {
                throw new RuntimeException("Unknown or column in output table: " + col);
            }
            assignments.add(col + "Setter.set(" + setter + ");"); }
        );
        if (keyClass != null) {
            processUserFields(keyClass, columnToKeyField, columnNameToType, assignments, "record.key()");
        } else if (!columnToKeyField.isEmpty()) {
            throw new IllegalArgumentException("Key class not specified!");
        }
        if (valueClass != null) {
            processUserFields(valueClass, columnToValueField, columnNameToType, assignments, "record.value()");
        } else if (!columnToValueField.isEmpty()) {
            throw new IllegalArgumentException("Value class not specified!");
        }
        if (keyClass != null) {
            processUserMethods(keyClass, columnToKeyMethod, columnNameToType, assignments, "record.key()");
        } else if (!columnToKeyMethod.isEmpty()) {
            throw new IllegalArgumentException("Key class not specified!");
        }
        if (valueClass != null) {
            processUserMethods(valueClass, columnToValueMethod, columnNameToType, assignments, "record.value()");
        } else if (!columnToValueMethod.isEmpty()) {
            throw new IllegalArgumentException("Value class not specified!");
        }

        if (valueClass != null) {
            assignFields(columnNameToType, valueClass, "record.value()", assignments);
        }
        if (keyClass != null) {
            assignFields(columnNameToType, keyClass, "record.key()", assignments);
        }

        columnsUnmapped.forEach(columnNameToType::remove);

        // now do a fuzzy search for the remaining columns
        if (caseInsensitiveSearch) {
            if (valueClass != null) {
                searchFields(columnNameToType, valueClass, "record.value()", assignments);
            }

            if (keyClass != null) {
                searchFields(columnNameToType, keyClass, "record.key()", assignments);
            }
        }

        if (!columnNameToType.isEmpty() && !allowUnmapped) {
            // is nullability allowed?
            throw new RuntimeException("No fields found for columns: " + columnNameToType.entrySet());
        }

        final String writerHash = Math.abs(Arrays.hashCode(columnNames)) + "_" + Math.abs(Arrays.hashCode(columnTypes));
        final String className = "PojoAdapter" + writerHash + "_" + (keyClass == null ? "" : keyClass.getSimpleName()) + "_" + (valueClass == null ? "" : valueClass.getSimpleName());
        final String packageName = getClass().getPackage().getName() + ".gen.temp";
        final String fullName = packageName + "." + className;

        final StringBuilder classBuilder = new StringBuilder();
        final Indenter indenter = new Indenter();

        classBuilder.append("import " + ConsumerRecordToTableWriterAdapter.class.getCanonicalName() + ";\n");
        classBuilder.append("import " + RowSetter.class.getCanonicalName() + ";\n");
        classBuilder.append("import " + TableWriter.class.getCanonicalName() + ";\n");
        classBuilder.append("import " + ConsumerRecord.class.getCanonicalName() + ";\n");
        classBuilder.append("import " + IOException.class.getCanonicalName() + ";\n");

        classBuilder.append("\n");

        if (keyClass != null) {
            classBuilder.append("import " + keyClass.getCanonicalName() + ";\n");
        }
        if (valueClass != null) {
            classBuilder.append("import " + valueClass.getCanonicalName() + ";\n");
        }

        classBuilder.append("\n");

        classBuilder.append("public class " + className + " implements " + ConsumerRecordToTableWriterAdapter.class.getCanonicalName() + "{\n");
        classBuilder.append(indenter.increaseLevel()).append("// setter fields\n");

        for (final String column : writer.getColumnNames()) {
            if (columnsUnmapped.contains(column) || column.equals(offsetColumnName) || column.equals(timestampColumnName)) {
                continue;
            }
            classBuilder.append(indenter).append("private final RowSetter ").append(column).append("Setter;").append("\n");
        }

        classBuilder.append("\n");

        classBuilder.append(indenter).append("public ").append(className).append("(TableWriter writer) {\n");
        indenter.increaseLevel();
        for (final String column : writer.getColumnNames()) {
            if (columnsUnmapped.contains(column) || column.equals(offsetColumnName) || column.equals(timestampColumnName)) {
                continue;
            }
            classBuilder.append(indenter).append(column).append("Setter = writer.getSetter(\"").append(column).append("\");").append("\n");
        }
        classBuilder.append(indenter.decreaseLevel()).append("}\n");

        classBuilder.append("\n");
        classBuilder.append(indenter).append("@Override\n");
        classBuilder.append(indenter).append("public void consumeRecord(ConsumerRecord<?, ?> record) throws IOException {\n");

        classBuilder.append(indenter.increaseLevel()).append("// method body\n");

        assignments.forEach(x -> classBuilder.append(indenter).append(x).append("\n"));

        classBuilder.append(indenter.decreaseLevel()).append("}\n");
        classBuilder.append(indenter.decreaseLevel()).append("}\n");

        final String classBody = classBuilder.toString();

        if (printClassBody) {
            System.out.println(classBody);
        }

        final Class<?> uncastedClass = CompilerTools.compile(className, classBody, packageName);
        if (!ConsumerRecordToTableWriterAdapter.class.isAssignableFrom(uncastedClass)) {
            throw new IllegalStateException("Compiled class is not a " + ConsumerRecordToTableWriterAdapter.class);
        }
        // noinspection unchecked
        final Class<ConsumerRecordToTableWriterAdapter> compiledClass = (Class<ConsumerRecordToTableWriterAdapter>)uncastedClass;
        try {
            final Constructor<ConsumerRecordToTableWriterAdapter> constructor = compiledClass.getConstructor(TableWriter.class);
            compiledPojoAdapter = constructor.newInstance(writer);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Compiled class does not have a TableWriter constructor " + compiledClass);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Could not create POJO adapter class", e);
        }
    }

    private void processUserFields(Class<?> inputClass, Map<String, String> columnToField, Map<String, Class> columnNameToType, List<String> assignments, String varName) {
        columnToField.forEach((col, fieldName) -> {
            final Class<?> colType = columnNameToType.remove(col);
            if (colType == null) {
                throw new RuntimeException("Unknown or column in output table: " + col);
            }
            try {
                final Field field = inputClass.getField(fieldName);
                doFieldAssignment(varName, assignments, inputClass, col, colType, field);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException("Field " + fieldName + " does not exist in " + inputClass.getSimpleName());
            }
        });
    }

    private void processUserMethods(Class<?> inputClass, Map<String, String> columnToMethod, Map<String, Class> columnNameToType, List<String> assignments, String varName) {
        columnToMethod.forEach((col, methodName) -> {
            final Class<?> colType = columnNameToType.remove(col);
            if (colType == null) {
                throw new RuntimeException("Unknown or column in output table: " + col);
            }
            try {
                final Method method = inputClass.getMethod(methodName);
                doMethodAssignment(varName, assignments, inputClass, col, colType, method);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Method " + methodName + "() does not exist in " + inputClass.getSimpleName());
            }
        });
    }

    private void assignFields(Map<String, Class> columnNameToType, Class<?> inputType, String varName, List<String> assignments) {
        for (final Iterator<Map.Entry<String, Class>> it = columnNameToType.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<String, Class> entry = it.next();
            final String columnName = entry.getKey();
            final Class columnType = entry.getValue();

            // try a method followed by a getMethod
            Method method;
            try {
                method = inputType.getMethod(columnName);
            } catch (NoSuchMethodException e) {
                try {
                    method = inputType.getMethod("get" + columnName);
                } catch (NoSuchMethodException ex) {
                    method = null;
                }
            }

            if (method != null) {
                doMethodAssignment(varName, assignments, inputType, columnName, columnType, method);
                it.remove();
                continue;
            }

            // if there was no method, fall back to a simple field
            try {
                doFieldAssignment(varName, assignments, inputType, columnName, columnType, inputType.getField(columnName));
                it.remove();
            } catch (NoSuchFieldException ignored) {
            }
        }
    }

    private void searchFields(Map<String, Class> columnNameToType, Class<?> inputType, String varName, List<String> assignments) {
        final Map<String, Method> getMethods = Arrays.stream(inputType.getMethods()).filter(m -> m.getParameterTypes().length == 0).collect(Collectors.toMap(Method::getName, Function.identity()));
        final Map<String, Field> fields = Arrays.stream(inputType.getFields()).collect(Collectors.toMap(Field::getName, Function.identity()));

        for (final Iterator<Map.Entry<String, Class>> it = columnNameToType.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<String, Class> entry = it.next();
            final String columnName = entry.getKey();
            final Class columnType = entry.getValue();

            // try a method with the same name
            if (checkMethods(inputType, varName, assignments, getMethods, it, columnName, columnType, columnName)) {
                continue;
            }

            // now try a method prefixed with "get"
            final String getColumnName = "get" + columnName;
            if (checkMethods(inputType, varName, assignments, getMethods, it, columnName, columnType, getColumnName)) {
                continue;
            }

            // finally try a field
            checkFields(inputType, varName, assignments, fields, it, columnName, columnType, columnName);
        }
    }

    private boolean checkMethods(Class<?> inputType, String varName, List<String> assignments, Map<String, Method> getMethods, Iterator<Map.Entry<String, Class>> it, String columnName, Class columnType, String getColumnName) {
        final List<String> getMethodCandidates = getMethods.keySet().stream().filter(getColumnName::equalsIgnoreCase).collect(Collectors.toList());
        if (getMethodCandidates.size() == 1) {
            doMethodAssignment(varName, assignments, inputType, columnName, columnType, getMethods.get(getMethodCandidates.get(0)));
            it.remove();
            return true;
        } else if (getMethodCandidates.size() > 1) {
            throw new RuntimeException("Multiple candidate methods for column " + columnName + ": " + getMethodCandidates);
        }
        return false;
    }

    private boolean checkFields(Class<?> inputType, String varName, List<String> assignments, Map<String, Field> fields, Iterator<Map.Entry<String, Class>> it, String columnName, Class columnType, String getColumnName) {
        final List<String> fieldCandidates = fields.keySet().stream().filter(getColumnName::equalsIgnoreCase).collect(Collectors.toList());
        if (fieldCandidates.size() == 1) {
            doFieldAssignment(varName, assignments, inputType, columnName, columnType, fields.get(fieldCandidates.get(0)));
            it.remove();
            return true;
        } else if (fieldCandidates.size() > 1) {
            throw new RuntimeException("Multiple candidate fields for column " + columnName + ": " + fieldCandidates);
        }
        return false;
    }

    private void doFieldAssignment(String varName, List<String> assignments, Class<?> inputClass, String columnName, Class columnType, Field field) {
        final boolean isAssignable = ClassUtils.isAssignable(field.getType(), columnType);
        if (isAssignable) {
            assignments.add(columnName + "Setter.set(((" + inputClass.getCanonicalName() + ")" + varName + ")." + field.getName() + ");");
        } else if (columnType == String.class && CharSequence.class.isAssignableFrom(field.getType())) {
            final String tmpVar = columnName + "Tmp__";
            assignments.add("final " + field.getType().getCanonicalName() + " " + tmpVar + " = ((" + inputClass.getCanonicalName() + ")" + varName + ")." + field.getName() + ";");
            assignments.add(columnName + "Setter.set(" + tmpVar + " == null ? null : " + tmpVar + ".toString());");
        } else{
            throw new RuntimeException("Can not assign to column of type " + columnType + " from field of type " + field.getType() + " for column " + columnName);
        }
    }

    private void doMethodAssignment(String varName, List<String> assignments, Class inputClass, String columnName, Class columnType, Method method) {
        final boolean isAssignable = ClassUtils.isAssignable(method.getReturnType(), columnType);
        if (isAssignable) {
            assignments.add(columnName + "Setter.set(((" + inputClass.getCanonicalName() + ")" + varName + ")." + method.getName() + "());");
        } else if (columnType == String.class && CharSequence.class.isAssignableFrom(method.getReturnType())) {
            final String tmpVar = columnName + "Tmp__";
            assignments.add("final " + method.getReturnType().getCanonicalName() + " " + tmpVar + " = ((" + inputClass.getCanonicalName() + ")" + varName + ")." + method.getName() + "();");
            assignments.add(columnName + "Setter.set(" + tmpVar + " == null ? null : " + tmpVar + ".toString());");
        } else {
            throw new RuntimeException("Can not assign to column of type " + columnType + " from method " + method.getName() + " with return type of " + method.getReturnType() + " for column " + columnName);
        }
    }

    /**
     * A builder for the PojoConsumerRecordToTableWriterAdapter.
     */
    public static class Builder {
        private String offsetColumnName;
        private String timestampColumnName;
        private String kafkaPartitionColumnName;
        private Class<?> keyClass;
        private Class<?> valueClass;
        private boolean allowUnmapped = false;
        private boolean caseInsensitiveSearch = false;
        private final Set<String> columnsUnmapped = new HashSet<>();
        private final Map<String, String> columnToSetter = new LinkedHashMap<>();
        private final Map<String, String> columnToKeyMethodSetter = new LinkedHashMap<>();
        private final Map<String, String> columnToValueMethodSetter = new LinkedHashMap<>();
        private final Map<String, String> columnToKeyFieldSetter = new LinkedHashMap<>();
        private final Map<String, String> columnToValueFieldSetter = new LinkedHashMap<>();
        private boolean printClassBody = false;

        /**
         * Set the name of the column which stores the Kafka offset of the record.
         *
         * @param offsetColumnName the name of the column in the output table
         *
         * @return this builder
         */
        @NotNull public Builder offsetColumnName(@NotNull String offsetColumnName) {
            this.offsetColumnName = NameValidator.validateColumnName(offsetColumnName);
            return this;
        }

        /**
         * Set the name of the column which stores the Kafka timestamp of the record.
         *
         * @param timestampColumnName the name of the column in the output table
         *
         * @return this builder
         */
        @NotNull public Builder timestampColumnName(@NotNull String timestampColumnName) {
            this.timestampColumnName = NameValidator.validateColumnName(timestampColumnName);
            return this;
        }

        /**
         * Set the name of the column which stores the Kafka partition identifier of the record.
         *
         * @param kafkaPartitionColumnName the name of the column in the output table
         *
         * @return this builder
         */
        @NotNull public Builder kafkaPartitionColumnName(@NotNull String kafkaPartitionColumnName) {
            this.kafkaPartitionColumnName = NameValidator.validateColumnName(kafkaPartitionColumnName);
            return this;
        }

        /**
         * Set the class to use for deserialized keys.
         *
         * @param keyClass the class for deserialized keys
         *
         * @return this builder
         */
        @NotNull public Builder keyClass(@NotNull Class<?> keyClass) {
            this.keyClass = keyClass;
            return this;
        }

        /**
         * Set the class to use for deserialized values.
         *
         * @param valueClass the class for deserialized values
         *
         * @return this builder
         */
        @NotNull public Builder valueClass(@NotNull Class<?> valueClass) {
            this.valueClass = valueClass;
            return this;
        }

        /**
         * If set to true, unmapped columns are not an error.
         *
         * @param allowUnmapped should unmapped columns be ignored?
         *
         * @return this builder
         */
        @NotNull public Builder allowUnmapped(boolean allowUnmapped) {
            this.allowUnmapped = allowUnmapped;
            return this;
        }

        /**
         * If set to true, a case insensitive search for column mappings is performed.
         *
         * @param caseInsensitiveSearch should a case insensitive search for matching columns be performed?
         *
         * @return this builder
         */
        @NotNull public Builder caseInsensitiveSearch(boolean caseInsensitiveSearch) {
            this.caseInsensitiveSearch = caseInsensitiveSearch;
            return this;
        }

        /**
         * Specifies that the given column should not be mapped.
         *
         * @param allowUnmapped the column name that should remain unmapped
         *
         * @return this builder
         */
        @NotNull public Builder allowUnmapped(@NotNull String allowUnmapped) {
            if (isDefined(allowUnmapped)) {
                throw new RuntimeException("Column " + allowUnmapped + " is already defined!");
            }
            columnsUnmapped.add(NameValidator.validateColumnName(allowUnmapped));
            return this;
        }

        /**
         * Maps a column to a field in the key class
         *
         * @param column the column name in the output table
         * @param field a field name in the key class
         *

         * @return this builder
         */
        @NotNull public Builder addColumnToKeyField(@NotNull String column, @NotNull String field) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToKeyFieldSetter.put(NameValidator.validateColumnName(column), field);
            return this;
        }

        /**
         * Maps a column to a method in the key class
         *
         * @param column the column name in the output table
         * @param method a method name in the key class
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToKeyMethod(@NotNull String column, @NotNull String method) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToKeyMethodSetter.put(NameValidator.validateColumnName(column), method);
            return this;
        }

        /**
         * Maps a column to a method in the value class
         *
         * @param column the column name in the output table
         * @param field a method name in the value class
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToValueField(@NotNull String column, @NotNull String field) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToValueFieldSetter.put(NameValidator.validateColumnName(column), field);
            return this;
        }

        /**
         * Maps a column to a method in the value class
         *
         * @param column the column name in the output table
         * @param method a method name in the value class
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToValueMethod(@NotNull String column, @NotNull String method) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToValueMethodSetter.put(NameValidator.validateColumnName(column), method);
            return this;
        }

        /**
         * Maps a column to setter text in the compiled adapter class.
         *
         * The Kafka record will have the name "record", and can be referenced for example by:
         * {@code .addColumnToSetter("UserId", "((ksql.pageviews)record.value()).getUserid().toString()")}
         *
         * @param column the column name
         * @param setter the setter text
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToSetter(@NotNull String column, @NotNull String setter) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToSetter.put(NameValidator.validateColumnName(column), setter);
            return this;
        }

        @NotNull public Builder setPrintClassBody(boolean printClassBody) {
            this.printClassBody = printClassBody;
            return this;
        }

        private boolean isDefined(String column) {
            return columnsUnmapped.contains(column) || columnToSetter.containsKey(column) || columnToKeyMethodSetter.containsKey(column) || columnToKeyFieldSetter.containsKey(column) || columnToValueMethodSetter.containsKey(column) || columnToValueFieldSetter.containsKey(column);
        }

        /**
         * Create a factory that produces a PojoConsumerRecordToTableWriterAdapter for a TableWriter.
         *
         * @return the factory
         */
        Function<TableWriter, ConsumerRecordToTableWriterAdapter> buildFactory() {
            return (TableWriter tw) -> new PojoConsumerRecordToTableWriterAdapter<>(tw, kafkaPartitionColumnName, offsetColumnName,
                    timestampColumnName, keyClass, valueClass,
                    allowUnmapped, caseInsensitiveSearch, printClassBody, columnsUnmapped,
                    columnToKeyFieldSetter, columnToValueFieldSetter,
                    columnToKeyMethodSetter, columnToValueMethodSetter,
                    columnToSetter);
        }
    }

    @Override
    public void consumeRecord(ConsumerRecord<?, ?> record) throws IOException {
        if (kafkaPartitionColumnSetter != null) {
            kafkaPartitionColumnSetter.setInt(record.partition());
        }
        if (offsetColumnSetter != null) {
            offsetColumnSetter.setLong(record.offset());
        }
        if (timestampColumnSetter != null) {
            if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
                timestampColumnSetter.set(null);
            } else {
                timestampColumnSetter.set(DBTimeUtils.millisToTime(record.timestamp()));
            }
        }

        compiledPojoAdapter.consumeRecord(record);

        writer.writeRow();
    }
}
