package io.deephaven.db.util;

import java.util.function.Function;

public class IterableUtils {
    public static <T> String makeCommaSeparatedList(Iterable<T> s) {
        return appendCommaSeparatedList(new StringBuilder(), s).toString();
    }

    public static <T> String makeSeparatedList(Iterable<T> s, String separator, Function<T, String> renderer) {
        return appendSeparatedList(new StringBuilder(), s, separator, renderer).toString();
    }

    public static <T> StringBuilder appendCommaSeparatedList(StringBuilder sb, Iterable<T> s) {
        return appendSeparatedList(sb, s, ", ", Object::toString);
    }

    public static <T> StringBuilder appendSeparatedList(StringBuilder sb, Iterable<T> s, String separator,
            Function<T, String> renderer) {
        String currentSep = "";
        for (T element : s) {
            sb.append(currentSep);
            sb.append(renderer.apply(element));
            currentSep = separator;
        }
        return sb;
    }
}
