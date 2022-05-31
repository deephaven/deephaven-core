package io.deephaven.uri.resolver;

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

public final class UriResolvers {

    private final Set<UriResolver> resolvers;
    private final Map<String, Set<UriResolver>> map;

    public UriResolvers(Set<UriResolver> resolvers) {
        this.resolvers = Objects.requireNonNull(resolvers);
        map = new HashMap<>();
        for (UriResolver resolver : resolvers) {
            for (String scheme : resolver.schemes()) {
                final Set<UriResolver> set = map.computeIfAbsent(scheme, s -> new HashSet<>());
                set.add(resolver);
            }
        }
    }

    public Set<UriResolver> resolvers() {
        return resolvers;
    }

    public <T extends UriResolver> Optional<T> find(Class<T> clazz) {
        return resolvers()
                .stream()
                .filter(t -> clazz.equals(t.getClass()))
                .map(clazz::cast)
                .findFirst();
    }

    public UriResolver resolver(URI uri) {
        final List<UriResolver> resolvers = map.getOrDefault(uri.getScheme(), Collections.emptySet())
                .stream()
                .filter(t -> t.isResolvable(uri))
                .collect(Collectors.toList());
        if (resolvers.isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format("Unable to find resolver for uri '%s'", uri));
        } else if (resolvers.size() > 1) {
            final String classes = resolvers.stream()
                    .map(UriResolver::getClass)
                    .map(Class::toString)
                    .collect(Collectors.joining(",", "[", "]"));
            throw new UnsupportedOperationException(
                    String.format("Found multiple resolvers for uri '%s': %s", uri, classes));
        }
        return resolvers.get(0);
    }

    public Object resolve(URI uri) throws InterruptedException {
        return resolver(uri).resolve(uri);
    }
}
