package io.deephaven.server.test;

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
    "rawtypes"
})
public final class TestAuthModule_BindBasicAuthTestImplFactory implements Factory<TestAuthModule.BasicAuthTestImpl> {
  private final TestAuthModule module;

  public TestAuthModule_BindBasicAuthTestImplFactory(TestAuthModule module) {
    this.module = module;
  }

  @Override
  public TestAuthModule.BasicAuthTestImpl get() {
    return bindBasicAuthTestImpl(module);
  }

  public static TestAuthModule_BindBasicAuthTestImplFactory create(TestAuthModule module) {
    return new TestAuthModule_BindBasicAuthTestImplFactory(module);
  }

  public static TestAuthModule.BasicAuthTestImpl bindBasicAuthTestImpl(TestAuthModule instance) {
    return Preconditions.checkNotNullFromProvides(instance.bindBasicAuthTestImpl());
  }
}
