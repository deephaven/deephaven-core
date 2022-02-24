package io.deephaven.server.object;

import io.deephaven.engine.table.Table;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeLookup;

import javax.inject.Inject;
import java.util.Objects;
import java.util.Optional;

public final class TypeLookup {

    private final ObjectTypeLookup lookup;

    @Inject
    public TypeLookup(ObjectTypeLookup lookup) {
        this.lookup = Objects.requireNonNull(lookup);
    }

    public Optional<String> type(Object object) {
        if (object instanceof Table) {
            return Optional.of("Table");
        }
        return lookup.findObjectType(object).map(ObjectType::name);
    }
}
