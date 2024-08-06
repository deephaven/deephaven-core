package io.deephaven.server.runner;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import javax.annotation.processing.Generated;

@ScopeMetadata("javax.inject.Singleton")
@QualifierMetadata("javax.inject.Named")
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
public final class ServerBuilderInProcessModule_ServerNameFactory implements Factory<String> {
  @Override
  public String get() {
    return serverName();
  }

  public static ServerBuilderInProcessModule_ServerNameFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static String serverName() {
    return Preconditions.checkNotNullFromProvides(ServerBuilderInProcessModule.serverName());
  }

  private static final class InstanceHolder {
    private static final ServerBuilderInProcessModule_ServerNameFactory INSTANCE = new ServerBuilderInProcessModule_ServerNameFactory();
  }
}
