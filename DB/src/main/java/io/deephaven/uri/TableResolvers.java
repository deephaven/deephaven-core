package io.deephaven.uri;

import io.deephaven.db.tables.Table;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Singleton
public final class TableResolvers implements UriCreator {

    private final Set<TableResolver> resolvers;
    private final Map<String, TableResolver> map;

    @Inject
    public TableResolvers(Set<TableResolver> resolvers) {
        this.resolvers = Objects.requireNonNull(resolvers);
        map = new HashMap<>();
        for (TableResolver resolver : resolvers) {
            for (String scheme : resolver.schemes()) {
                final TableResolver existing = map.put(scheme, resolver);
                if (existing != null) {
                    throw new IllegalArgumentException(String.format("Overlapping resolvers: '%s' and '%s'",
                            existing.getClass(), resolver.getClass()));
                }
            }
        }
    }

    public Set<TableResolver> resolvers() {
        return resolvers;
    }

    public <T extends TableResolver> Optional<T> find(Class<T> clazz) {
        return resolvers()
                .stream()
                .filter(t -> clazz.equals(t.getClass()))
                .map(clazz::cast)
                .findFirst();
    }

    public TableResolver resolver(String scheme) {
        final TableResolver resolver = map.get(scheme);
        if (resolver == null) {
            throw new UnsupportedOperationException(
                    String.format("Unable to find table resolver for scheme '%s'", scheme));
        }
        return resolver;
    }

    public Table resolve(URI uri) throws InterruptedException {
        return resolver(uri.getScheme()).resolve(uri);
    }

    @Override
    public ResolvableUri create(String scheme, String rest) {
        return resolver(scheme).create(scheme, rest);
    }
}
