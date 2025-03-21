//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;

import java.util.Optional;

public final class NameMappingUtil {

    public static Optional<NameMapping> readNameMappingDefault(TableMetadata metadata) {
        final String nameMappingJson = metadata.property(TableProperties.DEFAULT_NAME_MAPPING, null);
        return nameMappingJson == null
                ? Optional.empty()
                : Optional.of(NameMappingParser.fromJson(nameMappingJson));
    }

    public static Optional<NameMapping> readNameMappingDefault(Table table) {
        final String nameMappingJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        return nameMappingJson == null
                ? Optional.empty()
                : Optional.of(NameMappingParser.fromJson(nameMappingJson));
    }
}
