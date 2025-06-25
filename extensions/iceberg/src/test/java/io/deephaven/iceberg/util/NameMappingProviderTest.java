//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.mapping.NameMapping;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NameMappingProviderTest {
    @Test
    void empty() {
        final NameMappingProvider.EmptyNameMapping actual = NameMappingProvider.empty();
        assertVisitSelf(actual)
                .isEqualTo(NameMappingProvider.empty())
                .isNotEqualTo(NameMappingProvider.of(NameMapping.empty()));
    }

    @Test
    void fromTable() {
        final NameMappingProvider.TableNameMapping actual = NameMappingProvider.fromTable();
        assertVisitSelf(actual)
                .isEqualTo(NameMappingProvider.fromTable())
                .isNotEqualTo(NameMappingProvider.empty());
    }

    @Test
    void direct() {
        final NameMapping actualNameMapping = NameMapping.empty();
        final NameMappingProvider.DirectNameMapping actual = NameMappingProvider.of(actualNameMapping);
        assertThat(actual.nameMapping()).isSameAs(actualNameMapping);
        assertVisitSelf(actual)
                .isEqualTo(NameMappingProvider.of(NameMapping.empty()))
                .isNotEqualTo(NameMappingProvider.empty());
    }

    static ObjectAssert<NameMappingProvider> assertVisitSelf(NameMappingProvider x) {
        // This may seem a little silly, but allows us to ensure walk is implemented correctly
        return assertThat(VisitSelf.of(x));
    }

    enum VisitSelf implements NameMappingProvider.Visitor<NameMappingProvider> {
        VISIT_SELF;

        public static NameMappingProvider of(NameMappingProvider provider) {
            return provider.walk(VISIT_SELF);
        }

        @Override
        public NameMappingProvider visit(NameMappingProvider.TableNameMapping tableNameMapping) {
            return tableNameMapping;
        }

        @Override
        public NameMappingProvider visit(NameMappingProvider.EmptyNameMapping emptyNameMapping) {
            return emptyNameMapping;
        }

        @Override
        public NameMappingProvider visit(NameMappingProvider.DirectNameMapping directNameMapping) {
            return directNameMapping;
        }
    }
}
