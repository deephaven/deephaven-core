package io.deephaven.server.uri;

import io.deephaven.engine.table.Table;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.uri.UriHelper;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolversInstance;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

/**
 * The parquet table resolver is able to resolve local parquet files, or directories for the scheme {@value #SCHEME},
 * into {@link Table tables}.
 *
 * <p>
 * For example, {@code parquet:///data/my-file.parquet} or {@code parquet:///data/my-dir}.
 *
 * <p>
 * For more advanced use cases, see {@link ParquetTools}.
 */
public final class ParquetTableResolver implements UriResolver {

    /**
     * The parquet scheme, {@code parquet}.
     */
    public static final String SCHEME = "parquet";

    private static final Set<String> SCHEMES = Collections.singleton(SCHEME);

    public static boolean isWellFormed(URI uri) {
        return SCHEME.equals(uri.getScheme()) && UriHelper.isLocalPath(uri);
    }

    public static ParquetTableResolver get() {
        return UriResolversInstance.get().find(ParquetTableResolver.class).get();
    }

    @Inject
    public ParquetTableResolver() {}

    @Override
    public Set<String> schemes() {
        return SCHEMES;
    }

    @Override
    public boolean isResolvable(URI uri) {
        return isWellFormed(uri);
    }

    @Override
    public Table resolve(URI uri) throws InterruptedException {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid parquet URI '%s'", uri));
        }
        return ParquetTools.readTable(uri.getPath());
    }
}
