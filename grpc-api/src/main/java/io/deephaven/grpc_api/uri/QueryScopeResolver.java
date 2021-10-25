package io.deephaven.grpc_api.uri;

import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.uri.TableResolver;
import io.deephaven.uri.TableResolversInstance;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The query scope table resolver is able to resolve {@link QueryScopeUri query scope URIs}.
 *
 * <p>
 * For example, {@code dh:///scope/my_table}.
 *
 * @see QueryScopeUri query scope URI format
 */
public final class QueryScopeResolver implements TableResolver {

    public static QueryScopeResolver get() {
        return TableResolversInstance.get().find(QueryScopeResolver.class).get();
    }

    private final GlobalSessionProvider globalSessionProvider;

    @Inject
    public QueryScopeResolver(GlobalSessionProvider globalSessionProvider) {
        this.globalSessionProvider = Objects.requireNonNull(globalSessionProvider);
    }

    @Override
    public Set<String> schemes() {
        return Collections.singleton(QueryScopeUri.SCHEME);
    }

    @Override
    public boolean isResolvable(URI uri) {
        return QueryScopeUri.isWellFormed(uri);
    }

    @Override
    public Table resolve(URI uri) {
        return resolve(QueryScopeUri.of(uri));
    }

    public Table resolve(QueryScopeUri uri) {
        return resolve(uri.variableName());
    }

    public Table resolve(String variableName) {
        final Object variable = globalSessionProvider.getGlobalSession().getVariable(variableName, null);
        return asTable(variable, "global query scope", variableName);
    }

    private Table asTable(Object value, String context, String fieldName) {
        if (!(value instanceof Table)) {
            throw new IllegalArgumentException(
                    String.format("Field '%s' in '%s' is not a Table, is %s", fieldName, context, value.getClass()));
        }
        return (Table) value;
    }
}
