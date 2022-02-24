package io.deephaven.server.uri;

import io.deephaven.csv.CsvTools;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolversInstance;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The csv table resolver is able to resolve CSV for schemes {@code csv+http}, {@code http+csv}, {@code csv+https},
 * {@code https+csv}, {@code csv+file}, {@code file+csv}, and {@code csv} into {@link Table tables}.
 *
 * <p>
 * For example, {@code csv+https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv}.
 *
 * <p>
 * For more advanced use cases, see {@link CsvTools}.
 */
public final class CsvTableResolver implements UriResolver {

    private static final Set<String> SCHEMES = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("csv+http", "http+csv", "csv+https", "https+csv", "csv+file", "file+csv", "csv")));

    public static CsvTableResolver get() {
        return UriResolversInstance.get().find(CsvTableResolver.class).get();
    }

    @Inject
    public CsvTableResolver() {}

    @Override
    public Set<String> schemes() {
        return SCHEMES;
    }

    @Override
    public boolean isResolvable(URI uri) {
        return SCHEMES.contains(uri.getScheme());
    }

    @Override
    public Table resolve(URI uri) {
        try {
            return read(uri);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Table read(URI uri) throws IOException {
        try {
            return CsvTools.readCsv(csvString(uri));
        } catch (CsvReaderException inner) {
            throw new RuntimeException("Caught exception", inner);

        }
    }

    private static String csvString(URI uri) {
        final String scheme = uri.getScheme();
        switch (scheme) {
            case "csv+http":
            case "http+csv":
                return replaceScheme(uri, "http");

            case "csv+https":
            case "https+csv":
                return replaceScheme(uri, "https");

            case "csv+file":
            case "file+csv":
            case "csv":
                return replaceScheme(uri, "file");

            default:
                throw new IllegalArgumentException(String.format("Unexpected scheme '%s'", scheme));
        }
    }

    private static String replaceScheme(URI other, String newScheme) {
        try {
            return new URI(newScheme, other.getUserInfo(), other.getHost(), other.getPort(), other.getPath(),
                    other.getQuery(), other.getFragment()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
