package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * A Deephaven application field URI.
 *
 * <p>
 * For example, {@code app:///my_application/f/my_table}.
 */
@Immutable
@SimpleStyle
public abstract class ApplicationUri extends ResolvableUriBase {

    public static final String SCHEME = "app";

    public static ApplicationUri of(String applicationId, String fieldName) {
        return ImmutableApplicationUri.of(applicationId, fieldName);
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

    public static ApplicationUri fromPath(String scheme, String rest) {
        return of(URI.create(String.format("%s:///%s", scheme, rest)));
    }

    /**
     * Parses the {@code URI} into an application URI. The format looks like {@code app:///${appId}/field/${fieldName}}.
     *
     * @param uri the uri
     * @return the application uri
     */
    public static ApplicationUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid application URI '%s'", uri));
        }
        final String[] parts = uri.getPath().substring(1).split("/");
        if (parts.length != 3 || !FieldUri.SCHEME.equals(parts[1])) {
            throw new IllegalArgumentException(String.format("Invalid application URI '%s'", uri));
        }
        return of(parts[0], parts[2]);
    }


    /**
     * The application id.
     *
     * @return the application id
     */
    @Parameter
    public abstract String applicationId();

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
        return String.format("%s:///%s/%s/%s", SCHEME, applicationId(), FieldUri.SCHEME, fieldName());
    }

    @Override
    public final List<String> toParts() {
        return Arrays.asList(SCHEME, applicationId(), FieldUri.SCHEME, fieldName());
    }

    @Check
    final void checkApplicationId() {
        if (!UriHelper.isUriSafe(applicationId())) {
            throw new IllegalArgumentException(String.format("Invalid application id '%s'", applicationId()));
        }
    }

    @Check
    final void checkFieldName() {
        if (!UriHelper.isUriSafe(fieldName())) {
            throw new IllegalArgumentException(String.format("Invalid field name '%s'", fieldName()));
        }
    }
}
