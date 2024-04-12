package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.auth.BasicAuthMarshaller;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

@ScopeMetadata("javax.inject.Singleton")
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
public final class TestAuthModule_BindBasicAuthFactory implements Factory<BasicAuthMarshaller> {
  private final TestAuthModule module;

  private final Provider<TestAuthModule.BasicAuthTestImpl> handlerProvider;

  public TestAuthModule_BindBasicAuthFactory(TestAuthModule module,
      Provider<TestAuthModule.BasicAuthTestImpl> handlerProvider) {
    this.module = module;
    this.handlerProvider = handlerProvider;
  }

  @Override
  public BasicAuthMarshaller get() {
    return bindBasicAuth(module, handlerProvider.get());
  }

  public static TestAuthModule_BindBasicAuthFactory create(TestAuthModule module,
      Provider<TestAuthModule.BasicAuthTestImpl> handlerProvider) {
    return new TestAuthModule_BindBasicAuthFactory(module, handlerProvider);
  }

  public static BasicAuthMarshaller bindBasicAuth(TestAuthModule instance,
      TestAuthModule.BasicAuthTestImpl handler) {
    return Preconditions.checkNotNullFromProvides(instance.bindBasicAuth(handler));
  }
}
