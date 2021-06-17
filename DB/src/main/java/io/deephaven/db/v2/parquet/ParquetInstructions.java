package io.deephaven.db.v2.parquet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ParquetInstructions {
    private Map<String, String> columnNameMapping;
    private Map<String, String> reverseColumnNameMapping;

    public ParquetInstructions() {
    }

    public String getColumnNameMapping(final String fromName) {
        if (columnNameMapping == null) {
            return null;
        }
        return columnNameMapping.get(fromName);
    }

    public Map<String, String> getColumnNameMappings() {
        if (columnNameMapping == null) {
            return Collections.emptyMap();
        }
        return columnNameMapping;
    }

    public Map<String, String> getReverseColumnNameMappings() {
        if (reverseColumnNameMapping == null) {
            return Collections.emptyMap();
        }
        return reverseColumnNameMapping;
    }

    public ParquetInstructions addColumnNameMapping(final String fromName, final String toName) {
        if (columnNameMapping == null) {
            columnNameMapping = new HashMap<>();
            reverseColumnNameMapping = new HashMap<>();
        }
        final String previousFrom = reverseColumnNameMapping.get(toName);
        if (previousFrom != null && !previousFrom.equals(fromName)) {
            throw new IllegalArgumentException("Cannot add a mapping to already mapped name=" + toName + " for column=" + previousFrom);
        }
        columnNameMapping.put(fromName, toName);
        reverseColumnNameMapping.put(toName, fromName);
        return this;
    }

    public static final class Read extends ParquetInstructions {
        @Override
        public Read addColumnNameMapping(final String fromName, final String toName) {
            super.addColumnNameMapping(fromName, toName);
            return this;
        }
    }

    public static final class Write extends ParquetInstructions {
        @Override
        public Write addColumnNameMapping(final String fromName, final String toName) {
            super.addColumnNameMapping(fromName, toName);
            return this;
        }
    }
}
