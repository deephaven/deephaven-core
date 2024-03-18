//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class ScopeStaticImpl implements Scope {

    public static Builder builder() {
        return ImmutableScopeStaticImpl.builder();
    }

    public static ScopeStaticImpl empty() {
        return builder().build();
    }

    public abstract List<TableInformation> tables();

    @Override
    public final Optional<TableInformation> table(List<String> qualifiedName) {
        // We could save this in a map, but O(n) shouldn't really hurt us here.
        return tables().stream().filter(t -> qualifiedName.equals(t.qualifiedName())).findAny();
    }

    @Check
    final void checkUniqueQualifiedNames() {
        final long distinctCount = tables().stream().map(TableInformation::qualifiedName).distinct().count();
        if (tables().size() != distinctCount) {
            throw new IllegalArgumentException();
        }
    }

    public interface Builder {

        Builder addTables(TableInformation element);

        Builder addTables(TableInformation... elements);

        Builder addAllTables(Iterable<? extends TableInformation> elements);

        ScopeStaticImpl build();
    }
}
