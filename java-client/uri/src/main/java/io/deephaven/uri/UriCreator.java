package io.deephaven.uri;

/**
 * The inner interface for creating {@link ResolvableUri resolvable URIs}, useful for {@link RemoteUri} where URIs are
 * embedded inside of other URIs.
 */
public interface UriCreator {

    ResolvableUri create(String scheme, String rest);
}
