package io.deephaven.uri;

import java.net.URI;
import java.util.regex.Matcher;

class RemoteApplicationUri {

    static boolean isWellFormed(URI uri) {
        return RemoteUri.isValidScheme(uri.getScheme())
                && UriHelper.isRemotePath(uri)
                && ApplicationUri.PATH_PATTERN.matcher(uri.getPath()).matches();
    }

    static RemoteUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException();
        }
        final Matcher matcher = ApplicationUri.PATH_PATTERN.matcher(uri.getPath());
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        final String appId = matcher.group(1);
        final String fieldName = matcher.group(2);
        final ApplicationUri applicationUri = ApplicationUri.of(appId, fieldName);
        return RemoteUri.of(DeephavenTarget.from(uri), applicationUri);
    }

    static String toString(DeephavenTarget target, ApplicationUri applicationUri) {
        return String.format("%s/%s/%s/%s/%s", target, ApplicationUri.APPLICATION, applicationUri.applicationId(),
                ApplicationUri.FIELD, applicationUri.fieldName());
    }
}
