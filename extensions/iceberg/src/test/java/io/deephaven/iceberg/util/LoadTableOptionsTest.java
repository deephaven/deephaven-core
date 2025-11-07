//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LoadTableOptionsTest {

    private static final TableIdentifier FOO_BAR = TableIdentifier.of("Foo", "Bar");

    @Test
    void basic() {
        final LoadTableOptions options = LoadTableOptions.builder().id(FOO_BAR).build();
        assertThat(options.id()).isEqualTo(FOO_BAR);
        assertThat(options.resolver()).isEqualTo(ResolverProvider.infer());
        assertThat(options.nameMapping()).isEqualTo(NameMappingProvider.fromTable());
    }

    @Test
    void custom() {
        final UnboundResolver resolver = UnboundResolver.builder()
                .definition(TableDefinition.of(ColumnDefinition.ofInt("Foo")))
                .build();
        final LoadTableOptions options = LoadTableOptions.builder()
                .id(FOO_BAR)
                .resolver(resolver)
                .nameMapping(NameMappingProvider.empty())
                .build();
        assertThat(options.id()).isEqualTo(FOO_BAR);
        assertThat(options.resolver()).isEqualTo(resolver);
        assertThat(options.nameMapping()).isEqualTo(NameMappingProvider.empty());
    }

    @Test
    void idString() {
        assertThat(LoadTableOptions.builder().id("Foo.Bar").build())
                .isEqualTo(LoadTableOptions.builder().id(FOO_BAR).build());
    }
}
