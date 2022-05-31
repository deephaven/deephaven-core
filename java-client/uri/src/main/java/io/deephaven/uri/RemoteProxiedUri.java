package io.deephaven.uri;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class RemoteProxiedUri {

    static final Pattern QUERY_PATTERN = Pattern.compile("^uri=(.+)$");

    static boolean isWellFormed(URI uri) {
        return RemoteUri.isValidScheme(uri.getScheme())
                && UriHelper.isRemoteQuery(uri)
                && QUERY_PATTERN.matcher(uri.getQuery()).matches();
    }

    static RemoteUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException();
        }
        final Matcher matcher = QUERY_PATTERN.matcher(uri.getQuery());
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        final URI innerUri = URI.create(matcher.group(1));
        return RemoteUri.isWellFormed(innerUri) ? RemoteUri.of(DeephavenTarget.from(uri), RemoteUri.of(innerUri))
                : RemoteUri.of(DeephavenTarget.from(uri), CustomUri.of(innerUri));
    }

    static String toString(DeephavenTarget target, RemoteUri uri) {
        return String.format("%s?uri=%s", target, uri);
    }

    static String toString(DeephavenTarget target, URI uri) {
        final String encoded;
        try {
            encoded = URLEncoder.encode(uri.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return String.format("%s?uri=%s", target, encoded);
    }
}
