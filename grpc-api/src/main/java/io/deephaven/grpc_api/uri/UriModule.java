package io.deephaven.grpc_api.uri;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.grpc_api.barrage.BarrageClientModule;

/**
 * Installs the {@link TableResolver table resolvers}. See each specific resolver for more information.
 *
 * @see BarrageTableResolver
 * @see QueryScopeResolver
 * @see ApplicationResolver
 * @see CsvTableResolver
 * @see ParquetTableResolver
 */
@Module(includes = {BarrageClientModule.class})
public interface UriModule {

    @Binds
    @IntoSet
    TableResolver bindQueryScopeResolver(QueryScopeResolver resolver);

    @Binds
    @IntoSet
    TableResolver bindApplicationResolver(ApplicationResolver resolver);

    @Binds
    @IntoSet
    TableResolver bindsBarrageTableResolver(BarrageTableResolver resolver);

    @Binds
    @IntoSet
    TableResolver bindCsvResolver(CsvTableResolver resolver);

    @Binds
    @IntoSet
    TableResolver bindParquetResolver(ParquetTableResolver resolver);
}
