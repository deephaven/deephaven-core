//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.uri;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolversInstance;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

/**
 * The query scope table resolver is able to resolve {@link QueryScopeUri query scope URIs}.
 *
 * <p>
 * For example, {@code dh:///scope/my_table}.
 *
 * @see QueryScopeUri query scope URI format
 */
public final class QueryScopeResolver implements UriResolver {

    public static QueryScopeResolver get() {
        return UriResolversInstance.get().find(QueryScopeResolver.class).get();
    }

    @Inject
    public QueryScopeResolver() {}

    @Override
    public Set<String> schemes() {
        return Collections.singleton(DeephavenUri.LOCAL_SCHEME);
    }

    @Override
    public boolean isResolvable(URI uri) {
        return QueryScopeUri.isWellFormed(uri);
    }

    @Override
    public Object resolve(URI uri) {
        return resolve(QueryScopeUri.of(uri));
    }

    public Object resolve(QueryScopeUri uri) {
        return resolve(uri.variableName());
    }

    public Object resolve(String variableName) {
        return ExecutionContext.getContext().getQueryScope().readParamValue(variableName, null);
    }
}
