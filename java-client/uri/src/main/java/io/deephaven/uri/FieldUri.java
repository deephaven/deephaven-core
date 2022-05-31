package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Deephaven field URI.
 *
 * <p>
 * For example, {@code dh:///field/my_table}.
 *
 * <p>
 * Note: unlike other URIs, this URI can't be resolved by itself - it must be embedded inside of a {@link RemoteUri},
 * whereby the remote URIs {@link RemoteUri#target() target} host will be used as the application id.
 */
@Immutable
@SimpleStyle
public abstract class FieldUri extends DeephavenUriBase {

    public static final Pattern PATH_PATTERN = Pattern.compile("^/field/(.+)$");

    public static FieldUri of(String fieldName) {
        return ImmutableFieldUri.of(fieldName);
    }

    public static boolean isValidScheme(String scheme) {
        return DeephavenUri.LOCAL_SCHEME.equals(scheme);
    }

    public static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme())
                && UriHelper.isLocalPath(uri)
                && PATH_PATTERN.matcher(uri.getPath()).matches();
    }

    /**
     * Parses the {@code URI} into a field URI. The format looks like {@code dh:///field/${fieldName}}.
     *
     * @param uri the URI
     * @return the field URI
     */
    public static FieldUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid field URI '%s'", uri));
        }
        final Matcher matcher = PATH_PATTERN.matcher(uri.getPath());
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        return of(matcher.group(1));
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
    public final String toString() {
        return String.format("%s:///%s/%s", DeephavenUri.LOCAL_SCHEME, ApplicationUri.FIELD, fieldName());
    }

    @Check
    final void checkFieldName() {
        if (!UriHelper.isUriSafe(fieldName())) {
            throw new IllegalArgumentException(String.format("Invalid field name '%s'", fieldName()));
        }
    }
}
