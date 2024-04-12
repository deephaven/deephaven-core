package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.auth.BasicAuthMarshaller;
import java.util.Map;
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
public final class TestAuthModule_BindAuthHandlerMapFactory implements Factory<Map<String, AuthenticationRequestHandler>> {
  private final TestAuthModule module;

  private final Provider<BasicAuthMarshaller> basicAuthMarshallerProvider;

  public TestAuthModule_BindAuthHandlerMapFactory(TestAuthModule module,
      Provider<BasicAuthMarshaller> basicAuthMarshallerProvider) {
    this.module = module;
    this.basicAuthMarshallerProvider = basicAuthMarshallerProvider;
  }

  @Override
  public Map<String, AuthenticationRequestHandler> get() {
    return bindAuthHandlerMap(module, basicAuthMarshallerProvider.get());
  }

  public static TestAuthModule_BindAuthHandlerMapFactory create(TestAuthModule module,
      Provider<BasicAuthMarshaller> basicAuthMarshallerProvider) {
    return new TestAuthModule_BindAuthHandlerMapFactory(module, basicAuthMarshallerProvider);
  }

  public static Map<String, AuthenticationRequestHandler> bindAuthHandlerMap(
      TestAuthModule instance, BasicAuthMarshaller basicAuthMarshaller) {
    return Preconditions.checkNotNullFromProvides(instance.bindAuthHandlerMap(basicAuthMarshaller));
  }
}
