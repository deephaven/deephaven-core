//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public interface NameMappingProvider {

    /**
     * A name mapping from the {@link Table#properties() Table property} {@value TableProperties#DEFAULT_NAME_MAPPING}.
     * If the property does not exist, an {@link NameMapping#empty() empty} name mapping will be used.
     *
     * @return the "from table" name mapping
     * @see <a href="https://iceberg.apache.org/spec/#column-projection">schema.name-mapping.default</a>
     */
    static NameMappingProvider fromTable() {
        return new NameMappingProviderImpl() {
            @Override
            NameMapping create(Table table) {
                return NameMappingUtil.readNameMappingDefault(table).orElse(NameMapping.empty());
            }
        };
    }

    /**
     * An explicit name mapping.
     *
     * @param nameMapping the name mapping
     * @return the explicit name mapping
     * @see MappingUtil
     */
    static NameMappingProvider of(@NotNull final NameMapping nameMapping) {
        Objects.requireNonNull(nameMapping);
        return new NameMappingProviderImpl() {
            @Override
            NameMapping create(Table table) {
                return nameMapping;
            }
        };
    }

    /**
     * An empty name mapping.
     *
     * <p>
     * Equivalent to {@code of(NameMapping.empty())}.
     *
     * @return the empty name mapping
     */
    static NameMappingProvider empty() {
        return of(NameMapping.empty());
    }
}
