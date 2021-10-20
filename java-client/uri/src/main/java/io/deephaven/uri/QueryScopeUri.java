package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * A Deephaven query scope URI.
 *
 * <p>
 * For example, {@code scope:///my_table}.
 *
 * @see #of(URI) parsing logic
 */
@Immutable
@SimpleStyle
public abstract class QueryScopeUri extends ResolvableUriBase {

    /**
     * The query scope scheme, {@code scope}.
     */
    public static final String SCHEME = "scope";

    public static QueryScopeUri of(String variableName) {
        return ImmutableQueryScopeUri.of(variableName);
    }

    public static boolean isValidScheme(String scheme) {
        return SCHEME.equals(scheme);
    }

    public static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme())
                && uri.getHost() == null
                && !uri.isOpaque()
                && uri.getPath().charAt(0) == '/'
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    public static QueryScopeUri fromPath(String scheme, String rest) {
        return of(URI.create(String.format("%s:///%s", scheme, rest)));
    }

    /**
     * Parses the {@code URI} into a query scope URI. The format looks like {@code scope:///${variableName}}.
     *
     * @param uri the uri
     * @return the query scope uri
     */
    public static QueryScopeUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid query scope URI '%s'", uri));
        }
        return of(uri.getPath().substring(1));
    }

    /**
     * The variable name.
     *
     * @return the variable name
     */
    @Parameter
    public abstract String variableName();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final URI toUri() {
        return URI.create(toString());
    }

    @Override
    public final String scheme() {
        return SCHEME;
    }

    @Override
    public final List<String> toParts() {
        return Arrays.asList(SCHEME, variableName());
    }

    @Override
    public final String toString() {
        return String.format("%s:///%s", SCHEME, variableName());
    }

    @Check
    final void checkVariableName() {
        if (!UriHelper.isUriSafe(variableName())) {
            throw new IllegalArgumentException(String.format("Invalid variable name '%s'", variableName()));
        }
    }
}
