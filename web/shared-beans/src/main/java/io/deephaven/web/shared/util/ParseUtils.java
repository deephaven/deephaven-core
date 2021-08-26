package io.deephaven.web.shared.util;

public class ParseUtils {
    /**
     * This method exists because Java's Boolean.parseBoolean is too permissive (that method maps "true" to true, any
     * other string to false).
     * 
     * @return
     *         <ul>
     *         <li>"true" -> true</li>
     *         <li>"false" -> false</li>
     *         <li>any other string -> exception</li>
     *         <li>null -> exception</li>
     *         </ul>
     */
    public static boolean parseBoolean(String text) {
        if ("true".equalsIgnoreCase(text)) {
            return true;
        }
        if ("false".equalsIgnoreCase(text)) {
            return false;
        }
        throw new IllegalArgumentException("Can't parse as boolean: " + text);
    }
}
