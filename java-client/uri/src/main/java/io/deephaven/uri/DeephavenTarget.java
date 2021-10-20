package io.deephaven.uri;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

/**
 * A Deephaven target represents the information necessary to establish a connection to a remote Deephaven service.
 *
 * <p>
 * A Deephaven target has a {@link #scheme() scheme}, {@link #host() host}, and optional {@link #port() port}.
 *
 * <p>
 * The scheme must be {@link #TLS_SCHEME dh}, for TLS; or {@link #PLAINTEXT_SCHEME dh+plain}, for plaintext.
 *
 * @see #of(URI) parsing logic
 */
@Immutable
@BuildableStyle
public abstract class DeephavenTarget {

    /**
     * The scheme for TLS, {@code dh}.
     */
    public static final String TLS_SCHEME = "dh";

    /**
     * The scheme for plaintext, {@code dh+plain}.
     */
    public static final String PLAINTEXT_SCHEME = "dh+plain";

    public static Builder builder() {
        return ImmutableDeephavenTarget.builder();
    }

    public static DeephavenTarget of(String scheme, String host) {
        switch (scheme) {
            case TLS_SCHEME:
                return builder().isTLS(true).host(host).build();
            case PLAINTEXT_SCHEME:
                return builder().isTLS(false).host(host).build();
            default:
                throw new IllegalArgumentException(String.format("Invalid Deephaven target scheme '%s'", scheme));
        }
    }

    public static DeephavenTarget of(String scheme, String host, int port) {
        switch (scheme) {
            case TLS_SCHEME:
                return builder().isTLS(true).host(host).port(port).build();
            case PLAINTEXT_SCHEME:
                return builder().isTLS(false).host(host).port(port).build();
            default:
                throw new IllegalArgumentException(String.format("Invalid Deephaven target scheme '%s'", scheme));
        }
    }

    /**
     * Returns true if the scheme is valid for a Deephaven target.
     *
     * <p>
     * The valid schemes are {@link #TLS_SCHEME dh} and {@link #PLAINTEXT_SCHEME dh+plain}.
     *
     * @param scheme the scheme
     * @return true iff scheme is valid for Deephaven target
     *
     */
    public static boolean isValidScheme(String scheme) {
        return TLS_SCHEME.equals(scheme) || PLAINTEXT_SCHEME.equals(scheme);
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
        final int port = targetUri.getPort();
        return port == -1 ? of(targetUri.getScheme(), targetUri.getHost())
                : of(targetUri.getScheme(), targetUri.getHost(), port);
    }

    /**
     * The scheme. {@code dh} when {@link #isTLS() TLS} is enabled, {@code dh+plain} otherwise.
     *
     * @return the scheme
     */
    public final String scheme() {
        return isTLS() ? TLS_SCHEME : PLAINTEXT_SCHEME;
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

    public final List<String> toParts() {
        return Arrays.asList(scheme(), authority());
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
