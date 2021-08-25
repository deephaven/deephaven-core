package io.deephaven.properties;

import static io.deephaven.properties.PropertyVisitor.SEPARATOR;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A splayed path is a {@link PropertySet} where the fully resolved file paths represent property
 * keys and the corresponding contents of each file represents the property value. It is meant to
 * represent a standardized interface for property keys and values that can be read or written to
 * from a variety of tools. It is straightforward in the sense that there is no order-dependent
 * parsing logic.
 */
public class SplayedPath {

    private static final char FS_SEPARATOR = '/';
    private static final Pattern SEPARATOR_PATTERN =
        Pattern.compile(Character.toString(SEPARATOR), Pattern.LITERAL);
    private static final String VALUE_NAME = "__value";
    private static final Path VALUE_PATH = Paths.get(VALUE_NAME);

    private final Path path;

    private final boolean trim;

    /**
     * Property keys are essentially "flat" keys, and that doesn't mesh well with a filesystem
     * directory/file structure when one key is a prefix of another key. For example, the JVM might
     * have the system properties {@code file.encoding} and {@code file.encoding.pkg}. We can't have
     * both {@code <path>/file/encoding} and {@code <path>/file/encoding/pkg} as files. To work
     * around this, we can append a specific filename to the filesystem paths as such:
     * {@code <path>/file/encoding/__value} and {@code <path>/file/encoding/pkg/__value}.
     *
     * <p>
     * If writing/reading unrestricted properties (such as system properties), a value based
     * approach should be taken.
     *
     * <p>
     * If writing/reading restricted properties (ie, if we place a no-prefix restriction on
     * application properties), a non-value based approach can be taken.
     */
    private final boolean isValueBased;

    private final Writer writer;

    public SplayedPath(Path path, boolean trim, boolean isValueBased) {
        this.path = Objects.requireNonNull(path);
        if (!path.getFileSystem().equals(FileSystems.getDefault())) {
            throw new UnsupportedOperationException(String.format(
                "Expected path to be a default filesystem path. Instead is: %s",
                path.getFileSystem()));
        }
        this.trim = trim;
        this.isValueBased = isValueBased;
        this.writer = new Writer();
    }

    // ------------------------------- PropertyVisitor --------------------------------------------

    public boolean exists() {
        return Files.isDirectory(path);
    }

    private void check() throws IOException {
        if (!exists()) {
            throw new IOException(
                String.format("Path does not exist, or is not a directory: '%s'", path));
        }
    }

    public void write(PropertySet properties) throws IOException {
        check();
        try {
            writer.visitProperties(properties);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    // If we need more fine-grained write access in the future, we may want to expose this.
    /**
     * A {@link PropertyVisitor} writer to the splayed path. The visitor may throw
     * {@link UncheckedIOException}s.
     *
     * <p>
     * Prefer {@link #write(PropertySet)} if applicable.
     *
     * @return the visitor
     * @throws IOException
     */
    /*
     * public PropertyVisitor asUnsafeWriter() throws IOException { check(); return writer; }
     */

    /**
     * This visitor is used only *after* {@link #check()} has been invoked.
     */
    private class Writer extends PropertyVisitorStringBase {

        @Override
        public void visit(String key, String value) {
            // the unsafe impl assumes that check() has already been called
            try {
                write(key, value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void write(String key, String value) throws IOException {
            final Path path = stringToPath(key);
            Files.createDirectories(path.getParent());
            Files.write(path, toBytes(value));
        }
    }

    // ------------------------------- PropertySet ------------------------------------------------

    public Map<String, String> toStringMap() throws IOException {
        final Map<String, String> map = new LinkedHashMap<>();
        readTo(PropertyVisitor.of(map::put));
        return map;
    }

    public void readTo(PropertyVisitor visitor) throws IOException {
        try (final Stream<Path> stream = Files.walk(path, FileVisitOption.FOLLOW_LINKS)) {
            final Iterator<Path> it = stream.iterator();
            while (it.hasNext()) {
                final Path key = it.next();
                if (isValueBased && !key.endsWith(VALUE_PATH)) {
                    continue;
                }
                final BasicFileAttributes attributes = Files
                    .readAttributes(key, BasicFileAttributes.class);
                if (!attributes.isRegularFile()) {
                    continue;
                }
                final String keyString = pathToString(key);
                final String value = toString(Files.readAllBytes(key));
                visitor.visit(keyString, value);
            }
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    // If we need more fine-grained read access in the future, we may want to expose this.
    /**
     * A {@link PropertySet} reader of the splayed path. The property set may throw a
     * {@link UncheckedIOException}s.
     *
     * <p>
     * Prefer {@link #readTo(PropertyVisitor)} if applicable.
     *
     * @return the property set
     */
    /*
     * public PropertySet asUnsafePropertySet() { return visitor -> { try { readTo(visitor); } catch
     * (IOException e) { throw new UncheckedIOException(e); } }; }
     */

    // --------------------------------------------------------------------------------------------

    private byte[] toBytes(String value) {
        return (trim ? value.trim() : value).getBytes(StandardCharsets.UTF_8);
    }

    private String toString(byte[] bytes) {
        return trim ? new String(bytes, StandardCharsets.UTF_8).trim()
            : new String(bytes, StandardCharsets.UTF_8);
    }

    private String pathToString(Path key) {
        if (isValueBased) {
            if (!key.endsWith(VALUE_PATH)) {
                throw new IllegalStateException(
                    String.format("Expected path to be a value path, is not: '%s'", key));
            }
            key = key.getParent();
        }
        final String relative = path.relativize(key).toString();
        if (relative.indexOf(SEPARATOR) != -1) {
            throw new IllegalStateException(String.format(
                "Unable to translate path that has '%s' in it.", SEPARATOR));
        }
        return relative.replace(FS_SEPARATOR, SEPARATOR);
    }

    private Path stringToPath(String key) {
        Path next = path;
        for (String part : SEPARATOR_PATTERN.split(key)) {
            next = next.resolve(part);
        }
        return isValueBased ? next.resolve(VALUE_NAME) : next;
    }
}
