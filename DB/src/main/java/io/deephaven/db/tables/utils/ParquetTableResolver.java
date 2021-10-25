package io.deephaven.db.tables.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.uri.AdHocTableResolverBase;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

/**
 * The parquet table resolver is able to resolve local parquet files for the scheme {@link #SCHEME parquet}.
 *
 * <p>
 * For example, {@code parquet:///data/my-file.parquet}.
 *
 * <p>
 * For more advanced use cases, see {@link ParquetTools}.
 */
public final class ParquetTableResolver extends AdHocTableResolverBase {

    /**
     * The parquet scheme, {@code parquet}.
     */
    public static final String SCHEME = "parquet";

    private static final Set<String> SCHEMES = Collections.singleton(SCHEME);

    public static boolean isWellFormed(URI uri) {
        return SCHEME.equals(uri.getScheme())
                && uri.getHost() == null
                && !uri.isOpaque()
                && uri.getPath().charAt(0) == '/'
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
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
