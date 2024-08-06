package io.deephaven.server.jetty;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import javax.annotation.processing.Generated;

@ScopeMetadata("javax.inject.Singleton")
@QualifierMetadata
@DaggerGenerated
@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes",
    "KotlinInternal",
    "KotlinInternalInJava",
    "cast"
})
public final class JettyServerModule_ProvidesJsPluginRegistrationFactory implements Factory<JsPlugins> {
  @Override
  public JsPlugins get() {
    return providesJsPluginRegistration();
  }

  public static JettyServerModule_ProvidesJsPluginRegistrationFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static JsPlugins providesJsPluginRegistration() {
    return Preconditions.checkNotNullFromProvides(JettyServerModule.providesJsPluginRegistration());
  }

  private static final class InstanceHolder {
    private static final JettyServerModule_ProvidesJsPluginRegistrationFactory INSTANCE = new JettyServerModule_ProvidesJsPluginRegistrationFactory();
  }
}
