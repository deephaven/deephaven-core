package io.deephaven.uri;

import java.net.URI;
import java.util.regex.Matcher;

class RemoteFieldUri {

    static boolean isWellFormed(URI uri) {
        return RemoteUri.isValidScheme(uri.getScheme())
                && UriHelper.isRemotePath(uri)
                && FieldUri.PATH_PATTERN.matcher(uri.getPath()).matches();
    }

    static RemoteUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException();
        }
        final Matcher matcher = FieldUri.PATH_PATTERN.matcher(uri.getPath());
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        final String fieldName = matcher.group(1);
        final FieldUri fieldUri = FieldUri.of(fieldName);
        return RemoteUri.of(DeephavenTarget.from(uri), fieldUri);
    }

    static String toString(DeephavenTarget target, FieldUri fieldUri) {
        return String.format("%s/%s/%s", target, ApplicationUri.FIELD, fieldUri.fieldName());
    }
}
