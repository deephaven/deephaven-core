package io.deephaven.grpc_api.uri;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.client.impl.BarrageUriModule;
import io.deephaven.db.tables.utils.CsvTableResolver;
import io.deephaven.db.tables.utils.ParquetTableResolver;
import io.deephaven.grpc_api.barrage.BarrageClientModule;
import io.deephaven.uri.TableResolver;

/**
 * Installs the {@link TableResolver table resolvers}. See each specific resolver for more information.
 *
 * @see io.deephaven.client.impl.BarrageTableResolver
 * @see QueryScopeResolver
 * @see ApplicationResolver
 * @see CsvTableResolver
 * @see ParquetTableResolver
 */
@Module(includes = {BarrageClientModule.class, BarrageUriModule.class})
public interface UriModule {

    @Binds
    @IntoSet
    TableResolver bindQueryScopeResolver(QueryScopeResolver resolver);

    @Binds
    @IntoSet
    TableResolver bindApplicationResolver(ApplicationResolver resolver);

    @Binds
    @IntoSet
    TableResolver bindCsvResolver(CsvTableResolver resolver);

    @Binds
    @IntoSet
    TableResolver bindParquetResolver(ParquetTableResolver resolver);
}
