package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A remote Deephaven URI represents a structured link for resolving remote Deephaven resources. Is composed of a
 * {@link #target() target} and remote {@link #uri() uri}.
 *
 * <p>
 * For example, {@code dh://host/scope/my_table}.
 *
 * @see #of(UriCreator, URI) parsing logic
 */
@Immutable
@SimpleStyle
public abstract class RemoteUri extends ResolvableUriBase {

    public static RemoteUri of(DeephavenTarget target, ResolvableUri uri) {
        return ImmutableRemoteUri.of(target, uri);
    }

    public static boolean isValidScheme(String scheme) {
        return DeephavenTarget.isValidScheme(scheme);
    }

    public static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme())
                && uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().charAt(0) == '/'
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    public static RemoteUri fromPath(UriCreator resolver, String scheme, String rest) {
        return of(resolver, URI.create(String.format("%s://%s", scheme, rest)));
    }

    /**
     * Parses the {@code uri} into a remote URI.
     *
     * <p>
     * The format looks like {@code dh://host:port/${scheme}/${rest}}. The exact format of {@code rest} depends on the
     * {@code scheme}.
     *
     * @param creator the creator
     * @param uri the uri
     * @return the remote URI
     */
    public static RemoteUri of(UriCreator creator, URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid remote Deephaven URI '%s'", uri));
        }
        final int port = uri.getPort();
        final DeephavenTarget target;
        if (port == -1) {
            target = DeephavenTarget.of(uri.getScheme(), uri.getHost());
        } else {
            target = DeephavenTarget.of(uri.getScheme(), uri.getHost(), port);
        }

        // Strip absolute path '/' from URI
        final String rawPath = uri.getRawPath().substring(1);
        final int sep = rawPath.indexOf('/');
        if (sep == -1) {
            throw new IllegalArgumentException("Unable to find scheme / path separator");
        }
        final String scheme = rawPath.substring(0, sep);
        final String rest = rawPath.substring(sep + 1);
        return of(creator, target, scheme, rest);
    }

    public static RemoteUri of(
            UriCreator creator,
            DeephavenTarget target,
            String scheme,
            String rest) {
        return creator.create(scheme, rest).target(target);
    }

    /**
     * The Deephaven target.
     *
     * @return the target
     */
    @Parameter
    public abstract DeephavenTarget target();

    /**
     * The <em>remote</em> URI. As opposed to {@link #toUri()}, which represents {@code this} as a URI.
     *
     * @return the remote URI
     */
    @Parameter
    public abstract ResolvableUri uri();

    @Override
    public final URI toUri() {
        return URI.create(toString());
    }

    @Override
    public final List<String> toParts() {
        return Stream.concat(target().toParts().stream(), uri().toParts().stream()).collect(Collectors.toList());
    }

    @Override
    public final String scheme() {
        return target().scheme();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        final List<String> parts = uri().toParts();
        final String remotePath = String.join("/", parts);
        return String.format("%s://%s/%s", target().scheme(), target().authority(), remotePath);
    }
}
