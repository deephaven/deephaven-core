package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * A Deephaven field URI.
 *
 * <p>
 * For example, {@code field:///my_table}.
 *
 * <p>
 * Note: unlike other URIs, this URI can't be resolved by itself - it must be embedded inside of a {@link RemoteUri},
 * whereby the remote URIs {@link RemoteUri#target() target} host will be used as the application id.
 */
@Immutable
@SimpleStyle
public abstract class FieldUri extends ResolvableUriBase {

    /**
     * The field scheme, {@code field}.
     */
    public static final String SCHEME = "field";

    public static FieldUri of(String fieldName) {
        return ImmutableFieldUri.of(fieldName);
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

    public static FieldUri fromPath(String scheme, String rest) {
        return of(URI.create(String.format("%s:///%s", scheme, rest)));
    }

    /**
     * Parses the {@code URI} into a field URI. The format looks like {@code field:///${fieldName}}.
     *
     * @param uri the uri
     * @return the application uri
     */
    public static FieldUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid field URI '%s'", uri));
        }
        return of(uri.getPath().substring(1));
    }

    /**
     * The field name.
     *
     * @return the field name
     */
    @Parameter
    public abstract String fieldName();

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
    public final String toString() {
        return String.format("%s:///%s", SCHEME, fieldName());
    }

    @Override
    public final List<String> toParts() {
        return Arrays.asList(SCHEME, fieldName());
    }

    @Check
    final void checkFieldName() {
        if (!UriHelper.isUriSafe(fieldName())) {
            throw new IllegalArgumentException(String.format("Invalid field name '%s'", fieldName()));
        }
    }
}
