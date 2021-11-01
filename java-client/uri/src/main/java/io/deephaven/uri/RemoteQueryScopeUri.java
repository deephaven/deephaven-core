package io.deephaven.uri;

import java.net.URI;
import java.util.regex.Matcher;

class RemoteQueryScopeUri {

    static boolean isWellFormed(URI uri) {
        return RemoteUri.isValidScheme(uri.getScheme())
                && UriHelper.isRemotePath(uri)
                && QueryScopeUri.PATH_PATTERN.matcher(uri.getPath()).matches();
    }

    static RemoteUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException();
        }
        final Matcher matcher = QueryScopeUri.PATH_PATTERN.matcher(uri.getPath());
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        final String variableName = matcher.group(1);
        final QueryScopeUri queryScopeUri = QueryScopeUri.of(variableName);
        return RemoteUri.of(DeephavenTarget.from(uri), queryScopeUri);
    }

    static String toString(DeephavenTarget target, QueryScopeUri uri) {
        return String.format("%s/%s/%s", target, QueryScopeUri.SCOPE, uri.variableName());
    }
}
