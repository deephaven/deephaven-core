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
    static TableNameMapping fromTable() {
        return TableNameMapping.TABLE_NAME_MAPPING;
    }

    /**
     * An explicit name mapping.
     *
     * @param nameMapping the name mapping
     * @return the explicit name mapping
     * @see MappingUtil
     */
    static DirectNameMapping of(@NotNull final NameMapping nameMapping) {
        return new DirectNameMapping(nameMapping);
    }

    /**
     * An empty name mapping.
     *
     * <p>
     * Equivalent to {@code of(NameMapping.empty())}.
     *
     * @return the empty name mapping
     */
    static EmptyNameMapping empty() {
        return EmptyNameMapping.EMPTY_NAME_MAPPING;
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(TableNameMapping tableNameMapping);

        T visit(EmptyNameMapping emptyNameMapping);

        T visit(DirectNameMapping directNameMapping);
    }

    enum TableNameMapping implements NameMappingProvider {
        TABLE_NAME_MAPPING;

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    enum EmptyNameMapping implements NameMappingProvider {
        EMPTY_NAME_MAPPING;

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    final class DirectNameMapping implements NameMappingProvider {
        private final NameMapping nameMapping;

        private DirectNameMapping(NameMapping nameMapping) {
            this.nameMapping = Objects.requireNonNull(nameMapping);
        }

        public NameMapping nameMapping() {
            return nameMapping;
        }

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof DirectNameMapping))
                return false;
            DirectNameMapping that = (DirectNameMapping) o;
            return nameMapping.asMappedFields().equals(that.nameMapping.asMappedFields());
        }

        @Override
        public int hashCode() {
            return nameMapping.asMappedFields().hashCode();
        }

        @Override
        public String toString() {
            return "DirectNameMapping{" +
                    "nameMapping=" + nameMapping +
                    '}';
        }
    }
}
