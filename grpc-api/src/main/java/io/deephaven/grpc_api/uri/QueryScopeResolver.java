package io.deephaven.grpc_api.uri;

import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.uri.FieldUri;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.uri.RemoteUri;
import io.deephaven.uri.ResolvableUri;
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
 * For example, {@code scope:///my_table}.
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
    public Table resolve(URI uri) {
        return resolve(QueryScopeUri.of(uri));
    }

    @Override
    public QueryScopeUri create(String scheme, String rest) {
        return QueryScopeUri.fromPath(scheme, rest);
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

    /*
     * private class Resolver implements ResolvableUri.Visitor {
     * 
     * private Table out;
     * 
     * public Table out() { return Objects.requireNonNull(out); }
     * 
     * @Override public void visit(RemoteUri remoteUri) { throw new
     * UnsupportedOperationException(String.format("Unable to resolve '%s'", remoteUri)); }
     * 
     * @Override public void visit(URI uri) { throw new
     * UnsupportedOperationException(String.format("Unable to resolve '%s'", uri)); }
     * 
     * @Override public void visit(FieldUri fieldUri) { throw new
     * UnsupportedOperationException(String.format("Unable to resolve '%s'", fieldUri)); }
     * 
     * @Override public void visit(ApplicationUri applicationField) { throw new
     * UnsupportedOperationException(String.format("Unable to resolve '%s'", applicationField)); }
     * 
     * @Override public void visit(QueryScopeUri queryScope) { out = resolve(queryScope); } }
     */
}
