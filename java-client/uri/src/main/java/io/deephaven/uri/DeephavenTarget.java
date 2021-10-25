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
 * A Deephaven target has a {@link #scheme() scheme}, {@link #host() host}, and optional {@link #port() port}. When the
 * port is not specified, it's up to the client to determine the appropriate port (possibly by using a default port or
 * discovering the appropriate port to use).
 *
 * <p>
 * The scheme must be {@link DeephavenUri#TLS_SCHEME dh}, for TLS; or {@link DeephavenUri#PLAINTEXT_SCHEME dh+plain},
 * for plaintext.
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
     * The valid schemes are {@link DeephavenUri#TLS_SCHEME dh} and {@link DeephavenUri#PLAINTEXT_SCHEME dh+plain}.
     *
     * @param scheme the scheme
     * @return true iff scheme is valid for Deephaven target
     *
     */
    public static boolean isValidScheme(String scheme) {
        return DeephavenUri.isValidScheme(scheme);
    }

    public static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme())
                && uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().isEmpty()
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    /**
     * Parses the {@code targetUri} into a Deephaven target.
     *
     * <p>
     * The valid formats include {@code dh://host}, {@code dh://host:port}, {@code dh+plain://host}, and
     * {@code dh+plain://host:port}.
     *
     * @param targetUri the target uri
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
     * @param uri the uri
     * @return the Deephaven target
     */
    public static DeephavenTarget from(URI uri) {
        final int port = uri.getPort();
        if (port == -1) {
            return of(uri.getScheme(), uri.getHost());
        } else {
            return of(uri.getScheme(), uri.getHost(), port);
        }
    }

    public static DeephavenTarget of(String scheme, String host) {
        switch (scheme) {
            case DeephavenUri.TLS_SCHEME:
                return builder().isTLS(true).host(host).build();
            case DeephavenUri.PLAINTEXT_SCHEME:
                return builder().isTLS(false).host(host).build();
            default:
                throw new IllegalArgumentException(String.format("Invalid Deephaven target scheme '%s'", scheme));
        }
    }

    public static DeephavenTarget of(String scheme, String host, int port) {
        switch (scheme) {
            case DeephavenUri.TLS_SCHEME:
                return builder().isTLS(true).host(host).port(port).build();
            case DeephavenUri.PLAINTEXT_SCHEME:
                return builder().isTLS(false).host(host).port(port).build();
            default:
                throw new IllegalArgumentException(String.format("Invalid Deephaven target scheme '%s'", scheme));
        }
    }

    /**
     * The scheme. {@code dh} when {@link #isTLS() TLS} is enabled, {@code dh+plain} otherwise.
     *
     * @return the scheme
     */
    public final String scheme() {
        return isTLS() ? DeephavenUri.TLS_SCHEME : DeephavenUri.PLAINTEXT_SCHEME;
    }

    /**
     * The TLS flag.
     *
     * @return if TLS is enabled
     */
    public abstract boolean isTLS();

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
     * @return the uri
     */
    public final URI toUri() {
        return URI.create(toString());
    }

    public final String authority() {
        return port().isPresent() ? String.format("%s:%d", host(), port().getAsInt()) : host();
    }

    @Check
    final void checkHostPort() {
        // Will cause URI exception if port is invalid too
        if (!host().equals(toUri().getHost())) {
            throw new IllegalArgumentException(String.format("Invalid host '%s'", host()));
        }
    }

    @Override
    public final String toString() {
        return String.format("%s://%s", scheme(), authority());
    }

    public interface Builder {

        Builder host(String host);

        Builder port(int port);

        Builder isTLS(boolean useTLS);

        DeephavenTarget build();
    }
}
