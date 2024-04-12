package io.deephaven.client.impl;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import javax.annotation.processing.Generated;

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
public final class BarrageFactoryBuilderModule_ProvidesFactoryBuilderFactory implements Factory<BarrageSessionFactoryBuilder> {
  @Override
  public BarrageSessionFactoryBuilder get() {
    return providesFactoryBuilder();
  }

  public static BarrageFactoryBuilderModule_ProvidesFactoryBuilderFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static BarrageSessionFactoryBuilder providesFactoryBuilder() {
    return Preconditions.checkNotNullFromProvides(BarrageFactoryBuilderModule.providesFactoryBuilder());
  }

  private static final class InstanceHolder {
    private static final BarrageFactoryBuilderModule_ProvidesFactoryBuilderFactory INSTANCE = new BarrageFactoryBuilderModule_ProvidesFactoryBuilderFactory();
  }
}
