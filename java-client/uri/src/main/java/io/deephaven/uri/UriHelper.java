package io.deephaven.uri;

import java.io.UnsupportedEncodingException;
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
}
