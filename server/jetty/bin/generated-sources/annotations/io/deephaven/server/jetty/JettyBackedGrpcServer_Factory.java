package io.deephaven.server.jetty;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

@ScopeMetadata
@QualifierMetadata
@DaggerGenerated
@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class JettyBackedGrpcServer_Factory implements Factory<JettyBackedGrpcServer> {
  private final Provider<JettyConfig> configProvider;

  private final Provider<GrpcFilter> filterProvider;

  public JettyBackedGrpcServer_Factory(Provider<JettyConfig> configProvider,
      Provider<GrpcFilter> filterProvider) {
    this.configProvider = configProvider;
    this.filterProvider = filterProvider;
  }

  @Override
  public JettyBackedGrpcServer get() {
    return newInstance(configProvider.get(), filterProvider.get());
  }

  public static JettyBackedGrpcServer_Factory create(Provider<JettyConfig> configProvider,
      Provider<GrpcFilter> filterProvider) {
    return new JettyBackedGrpcServer_Factory(configProvider, filterProvider);
  }

  public static JettyBackedGrpcServer newInstance(JettyConfig config, GrpcFilter filter) {
    return new JettyBackedGrpcServer(config, filter);
  }
}
