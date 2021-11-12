package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Deephaven application field URI.
 *
 * <p>
 * For example, {@code dh:///app/my_application/field/my_table}.
 */
@Immutable
@SimpleStyle
public abstract class ApplicationUri extends DeephavenUriBase {

    public static final String APPLICATION = "app";

    public static final String FIELD = "field";

    public static final Pattern PATH_PATTERN = Pattern.compile("^/app/(.+)/field/(.+)$");

    public static ApplicationUri of(String applicationId, String fieldName) {
        return ImmutableApplicationUri.of(applicationId, fieldName);
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
     * Parses the {@code URI} into an application URI. The format looks like
     * {@code dh:///app/${appId}/field/${fieldName}}.
     *
     * @param uri the URI
     * @return the application URI
     */
    public static ApplicationUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid application URI '%s'", uri));
        }
        final Matcher matcher = PATH_PATTERN.matcher(uri.getPath());
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        final String appId = matcher.group(1);
        final String fieldName = matcher.group(2);
        return of(appId, fieldName);
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
    public final String toString() {
        return String.format("%s:///%s/%s/%s/%s", DeephavenUri.LOCAL_SCHEME, APPLICATION, applicationId(), FIELD,
                fieldName());
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
