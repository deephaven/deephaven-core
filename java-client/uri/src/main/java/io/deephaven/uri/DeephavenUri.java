package io.deephaven.uri;

/**
 * A Deephaven URI is a structure URI that has a Deephaven-specific scheme, either {@code dh} or {@code dh+plain}.
 *
 * @see ApplicationUri
 * @see FieldUri
 * @see QueryScopeUri
 * @see RemoteUri
 */
public interface DeephavenUri extends StructuredUri {

    /**
     * The scheme for {@link RemoteUri remote URIs} using TLS, {@code dh}.
     */
    String TLS_SCHEME = "dh";

    /**
     * The scheme for {@link RemoteUri remote URIs} using plaintext, {@code dh+plain}.
     */
    String PLAINTEXT_SCHEME = "dh+plain";

    /**
     * The scheme for non-remote {@link DeephavenUri Deephaven URIs}, {@code dh}.
     */
    String LOCAL_SCHEME = TLS_SCHEME;

    static boolean isValidScheme(String scheme) {
        return TLS_SCHEME.equals(scheme) || PLAINTEXT_SCHEME.equals(scheme);
    }
}
