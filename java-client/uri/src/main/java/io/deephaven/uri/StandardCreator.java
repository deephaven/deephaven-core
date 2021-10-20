package io.deephaven.uri;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A static set of the standard creators. Useful for static cases, or for testing.
 */
public enum StandardCreator implements UriCreator {
    INSTANCE;

    private static final Set<String> SCHEMES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            DeephavenTarget.TLS_SCHEME,
            DeephavenTarget.PLAINTEXT_SCHEME,
            QueryScopeUri.SCHEME,
            ApplicationUri.SCHEME,
            FieldUri.SCHEME)));

    public static boolean isStandardScheme(String scheme) {
        return SCHEMES.contains(scheme);
    }

    @Override
    public ResolvableUri create(String scheme, String rest) {
        switch (scheme) {
            case DeephavenTarget.TLS_SCHEME:
            case DeephavenTarget.PLAINTEXT_SCHEME:
                return RemoteUri.fromPath(this, scheme, rest);

            case QueryScopeUri.SCHEME:
                return QueryScopeUri.fromPath(scheme, rest);

            case ApplicationUri.SCHEME:
                return ApplicationUri.fromPath(scheme, rest);

            case FieldUri.SCHEME:
                return FieldUri.fromPath(scheme, rest);

            default:
                return RawUri.fromPath(scheme, rest);
        }
    }
}
