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
     * The scheme for {@link RemoteUri remote URIs} using secure connections, {@code dh}.
     */
    String SECURE_SCHEME = "dh";

    /**
     * The scheme for {@link RemoteUri remote URIs} using plaintext connections, {@code dh+plain}.
     */
    String PLAINTEXT_SCHEME = "dh+plain";

    /**
     * The scheme for non-remote {@link DeephavenUri Deephaven URIs}, {@code dh}.
     */
    String LOCAL_SCHEME = SECURE_SCHEME;

    static boolean isValidScheme(String scheme) {
        return SECURE_SCHEME.equals(scheme) || PLAINTEXT_SCHEME.equals(scheme);
    }
}
