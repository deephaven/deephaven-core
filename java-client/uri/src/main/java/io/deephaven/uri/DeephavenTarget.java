package io.deephaven.uri;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.net.URI;
import java.util.OptionalInt;

/**
 * A Deephaven target represents the information necessary to establish a connection to a remote Deephaven service.
 *
 * <p>
 * A Deephaven target has a {@link #isSecure() secure flag}, {@link #host() host}, and optional {@link #port() port}.
 * When the port is not specified, it's up to the client to determine the appropriate port (possibly by using a default
 * port or discovering the appropriate port to use).
 *
 * @see #of(URI) parsing logic
 */
@Immutable
@BuildableStyle
public abstract class DeephavenTarget {

    public static Builder builder() {
        return ImmutableDeephavenTarget.builder();
    }

    /**
     * Returns true if the scheme is valid for a Deephaven target.
     *
     * <p>
     * The valid schemes are {@value DeephavenUri#SECURE_SCHEME} and {@value DeephavenUri#PLAINTEXT_SCHEME}.
     *
     * @param scheme the scheme
     * @return true iff scheme is valid for Deephaven target
     *
     */
    public static boolean isValidScheme(String scheme) {
        return DeephavenUri.isValidScheme(scheme);
    }

    public static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme()) && UriHelper.isRemoteTarget(uri);
    }

    /**
     * Parses the {@code targetUri} into a Deephaven target.
     *
     * <p>
     * The valid formats include {@code dh://host}, {@code dh://host:port}, {@code dh+plain://host}, and
     * {@code dh+plain://host:port}.
     *
     * @param targetUri the target URI
     * @return the Deephaven target
     */
    public static DeephavenTarget of(URI targetUri) {
        if (!isWellFormed(targetUri)) {
            throw new IllegalArgumentException(String.format("Invalid target Deephaven URI '%s'", targetUri));
        }
        return from(targetUri);
    }

    /**
     * Parses the {@code uri} into a Deephaven target, without strict URI checks. Useful when parsing a Deephaven target
     * as part of a {@link StructuredUri structured URI}.
     *
     * @param uri the URI
     * @return the Deephaven target
     */
    public static DeephavenTarget from(URI uri) {
        final String scheme = uri.getScheme();
        final int port = uri.getPort();
        Builder builder = builder().host(uri.getHost());
        switch (scheme) {
            case DeephavenUri.SECURE_SCHEME:
                builder.isSecure(true);
                break;
            case DeephavenUri.PLAINTEXT_SCHEME:
                builder.isSecure(false);
                break;
            default:
                throw new IllegalArgumentException(String.format("Invalid Deephaven target scheme '%s'", scheme));
        }
        if (port != -1) {
            builder.port(port);
        }
        return builder.build();
    }

    /**
     * The secure flag, typically representing Transport Layer Security (TLS).
     *
     * @return true if secure
     */
    public abstract boolean isSecure();

    /**
     * The host or IP address.
     *
     * @return the host
     */
    public abstract String host();

    /**
     * The optional port.
     *
     * @return the port
     */
    public abstract OptionalInt port();

    /**
     * The target as a URI.
     *
     * @return the URI
     */
    public final URI toURI() {
        return URI.create(toString());
    }

    @Check
    final void checkHostPort() {
        // Will cause URI exception if port is invalid too
        if (!host().equals(toURI().getHost())) {
            throw new IllegalArgumentException(String.format("Invalid host '%s'", host()));
        }
    }

    @Override
    public final String toString() {
        final String scheme = isSecure() ? DeephavenUri.SECURE_SCHEME : DeephavenUri.PLAINTEXT_SCHEME;
        return port().isPresent() ? String.format("%s://%s:%d", scheme, host(), port().getAsInt())
                : String.format("%s://%s", scheme, host());
    }

    public interface Builder {

        Builder host(String host);

        Builder port(int port);

        Builder isSecure(boolean isSecure);

        DeephavenTarget build();
    }
}
