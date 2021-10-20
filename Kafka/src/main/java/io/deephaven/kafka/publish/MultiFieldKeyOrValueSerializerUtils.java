package io.deephaven.kafka.publish;

class MultiFieldKeyOrValueSerializerUtils {
    @FunctionalInterface
    interface StringStringProcedure {
        void apply(String columnName, String fieldName);
    }

    static void makeFieldProcessors(
            final String[] columnNames,
            final String[] fieldNames,
            final StringStringProcedure fieldProcessorMaker) {
        if (fieldNames != null && fieldNames.length != columnNames.length) {
            throw new IllegalArgumentException(
                    "fieldNames.length (" + fieldNames.length + ") != columnNames.length (" + columnNames.length + ")");
        }
        for (int i = 0; i < columnNames.length; ++i) {
            final String columnName = columnNames[i];
            try {
                if (fieldNames == null) {
                    fieldProcessorMaker.apply(columnName, columnName);
                } else {
                    fieldProcessorMaker.apply(columnName, fieldNames[i]);
                }
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Unknown column name " + columnName + " for table", e);
            }
        }
    }
}
