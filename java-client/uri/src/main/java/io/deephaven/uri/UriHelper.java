package io.deephaven.uri;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;

public class UriHelper {
    public static boolean isUriSafe(String part) {
        final String encoded;
        try {
            encoded = URLEncoder.encode(part, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return false;
        }
        return part.equals(encoded);
    }

    /**
     * A URI is a "local path" when the only components are {@link URI#getScheme() scheme} and {@link URI#getPath()
     * path}; and path starts with {@code "/"}.
     *
     * @param uri the URI
     * @return true if {@code uri} is a "local path"
     */
    public static boolean isLocalPath(URI uri) {
        return uri.getHost() == null
                && !uri.isOpaque()
                && uri.getPath().startsWith("/")
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    /**
     * A URI is a "remote path" when the only components are {@link URI#getScheme() scheme}, {@link URI#getHost() host},
     * and {@link URI#getPath() path}; and path starts with {@code "/"}.
     *
     * @param uri the URI
     * @return true if {@code uri} is a "remote path"
     */
    public static boolean isRemotePath(URI uri) {
        return uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().startsWith("/")
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    /**
     * A URI is a "remote target" when the only components are {@link URI#getScheme() scheme}, {@link URI#getHost()
     * host}, and {@link URI#getPath() path}; and path is empty.
     *
     * @param uri the URI
     * @return true if {@code uri} is a "remote target"
     */
    public static boolean isRemoteTarget(URI uri) {
        return uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().isEmpty()
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    /**
     * A URI is a "remote query" when the only components are {@link URI#getScheme() scheme}, {@link URI#getHost()
     * host}, {@link URI#getQuery() query}, and {@link URI#getPath() path}; and path is empty.
     *
     * @param uri the URI
     * @return true if {@code uri} is a "remote query"
     */
    public static boolean isRemoteQuery(URI uri) {
        return uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().isEmpty()
                && uri.getQuery() != null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }
}
