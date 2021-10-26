package io.deephaven.grpc_api.uri;

import io.deephaven.db.tables.Table;
import io.deephaven.uri.TableResolver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public final class TableResolvers {

    private final Set<TableResolver> resolvers;
    private final Map<String, Set<TableResolver>> map;

    @Inject
    public TableResolvers(Set<TableResolver> resolvers) {
        this.resolvers = Objects.requireNonNull(resolvers);
        map = new HashMap<>();
        for (TableResolver resolver : resolvers) {
            for (String scheme : resolver.schemes()) {
                final Set<TableResolver> set = map.computeIfAbsent(scheme, s -> new HashSet<>());
                set.add(resolver);
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

    public TableResolver resolver(URI uri) {
        final List<TableResolver> resolvers = map.getOrDefault(uri.getScheme(), Collections.emptySet())
                .stream()
                .filter(t -> t.isResolvable(uri))
                .limit(2)
                .collect(Collectors.toList());
        if (resolvers.isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format("Unable to find resolver for uri '%s'", uri));
        } else if (resolvers.size() > 1) {
            throw new UnsupportedOperationException(
                    String.format("Found multiple resolvers for uri '%s'", uri));
        }
        return resolvers.get(0);
    }

    public Table resolve(URI uri) throws InterruptedException {
        return resolver(uri).resolve(uri);
    }
}
