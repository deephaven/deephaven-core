package io.deephaven.server.uri;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.server.barrage.BarrageClientModule;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolvers;

import javax.inject.Singleton;
import java.util.Set;

/**
 * Installs the {@link UriResolver URI resolvers}. See each specific resolver for more information.
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
    UriResolver bindQueryScopeResolver(QueryScopeResolver resolver);

    @Binds
    @IntoSet
    UriResolver bindApplicationResolver(ApplicationResolver resolver);

    @Binds
    @IntoSet
    UriResolver bindsBarrageTableResolver(BarrageTableResolver resolver);

    @Binds
    @IntoSet
    UriResolver bindCsvResolver(CsvTableResolver resolver);

    @Binds
    @IntoSet
    UriResolver bindParquetResolver(ParquetTableResolver resolver);

    @Provides
    @Singleton
    static UriResolvers bindResolvers(Set<UriResolver> resolvers) {
        return new UriResolvers(resolvers);
    }
}
