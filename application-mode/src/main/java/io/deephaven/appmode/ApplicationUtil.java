package io.deephaven.appmode;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

public class ApplicationUtil {
    private static final String FILE_PROP_PREFIX = "file_";

    public static Path[] findFilesFrom(final Properties properties) {
        return properties.stringPropertyNames().stream().filter(ApplicationUtil::isFileProperty)
                .map(prop -> toOrderedFile(prop, Paths.get(properties.getProperty(prop))))
                .sorted()
                .map(of -> of.file)
                .toArray(Path[]::new);
    }

    public static boolean isEnabled(final Properties properties) {
        return Boolean.parseBoolean((String) properties.getOrDefault("enabled", "true"));
    }

    public static boolean isAsciiPrintable(final CharSequence cs) {
        if (cs == null) {
            return false;
        }
        int sz = cs.length();
        for (int i = 0; i < sz; ++i) {
            char ch = cs.charAt(i);
            if (ch < 32 || ch >= 127) {
                return false;
            }
        }
        return true;
    }

    private static boolean isFileProperty(final String propName) {
        if (!propName.startsWith(FILE_PROP_PREFIX)
                || propName.length() == FILE_PROP_PREFIX.length()) {
            return false;
        }

        for (int i = FILE_PROP_PREFIX.length(); i < propName.length(); ++i) {
            char ch = propName.charAt(i);
            if (ch < '0' || ch > '9') {
                return false;
            }
        }

        return true;
    }

    private static OrderedFile toOrderedFile(final String propName, final Path path) {
        final String orderString = propName.substring(FILE_PROP_PREFIX.length());
        return new OrderedFile(Integer.parseInt(orderString), path);
    }

    private static class OrderedFile implements Comparable<OrderedFile> {
        int order;
        Path file;

        OrderedFile(int order, Path file) {
            this.file = file;
            this.order = order;
        }

        @Override
        public int compareTo(final OrderedFile o) {
            return Integer.compare(order, o.order);
        }
    }
}
