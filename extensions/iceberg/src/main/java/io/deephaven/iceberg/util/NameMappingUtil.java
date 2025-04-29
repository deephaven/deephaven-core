//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;

import java.util.Map;
import java.util.Optional;

final class NameMappingUtil {

    public static Optional<NameMapping> readNameMappingDefault(Table table) {
        return readNameMappingDefault(table.properties());
    }

    public static Optional<NameMapping> readNameMappingDefault(Map<String, String> metadata) {
        final String nameMappingJson = metadata.getOrDefault(TableProperties.DEFAULT_NAME_MAPPING, null);
        return nameMappingJson == null
                ? Optional.empty()
                : Optional.of(NameMappingParser.fromJson(nameMappingJson));
    }
}
