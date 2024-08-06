package io.deephaven.server.runner;

import dagger.MembersInjector;
import dagger.internal.DaggerGenerated;
import dagger.internal.InjectedFieldSignature;
import dagger.internal.QualifierMetadata;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.util.Scheduler;
import io.grpc.ManagedChannelBuilder;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

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
public final class DeephavenApiServerTestBase_MembersInjector implements MembersInjector<DeephavenApiServerTestBase> {
  private final Provider<ExecutionContext> executionContextProvider;

  private final Provider<DeephavenApiServer> serverProvider;

  private final Provider<Scheduler.DelegatingImpl> schedulerProvider;

  private final Provider<ScriptSession> scriptSessionProvider;

  private final Provider<ManagedChannelBuilder<?>> managedChannelBuilderProvider;

  private final Provider<RpcServerStateInterceptor> serverStateInterceptorProvider;

  public DeephavenApiServerTestBase_MembersInjector(
      Provider<ExecutionContext> executionContextProvider,
      Provider<DeephavenApiServer> serverProvider,
      Provider<Scheduler.DelegatingImpl> schedulerProvider,
      Provider<ScriptSession> scriptSessionProvider,
      Provider<ManagedChannelBuilder<?>> managedChannelBuilderProvider,
      Provider<RpcServerStateInterceptor> serverStateInterceptorProvider) {
    this.executionContextProvider = executionContextProvider;
    this.serverProvider = serverProvider;
    this.schedulerProvider = schedulerProvider;
    this.scriptSessionProvider = scriptSessionProvider;
    this.managedChannelBuilderProvider = managedChannelBuilderProvider;
    this.serverStateInterceptorProvider = serverStateInterceptorProvider;
  }

  public static MembersInjector<DeephavenApiServerTestBase> create(
      Provider<ExecutionContext> executionContextProvider,
      Provider<DeephavenApiServer> serverProvider,
      Provider<Scheduler.DelegatingImpl> schedulerProvider,
      Provider<ScriptSession> scriptSessionProvider,
      Provider<ManagedChannelBuilder<?>> managedChannelBuilderProvider,
      Provider<RpcServerStateInterceptor> serverStateInterceptorProvider) {
    return new DeephavenApiServerTestBase_MembersInjector(executionContextProvider, serverProvider, schedulerProvider, scriptSessionProvider, managedChannelBuilderProvider, serverStateInterceptorProvider);
  }

  @Override
  public void injectMembers(DeephavenApiServerTestBase instance) {
    injectExecutionContext(instance, executionContextProvider.get());
    injectServer(instance, serverProvider.get());
    injectScheduler(instance, schedulerProvider.get());
    injectScriptSessionProvider(instance, scriptSessionProvider);
    injectManagedChannelBuilderProvider(instance, managedChannelBuilderProvider);
    injectServerStateInterceptor(instance, serverStateInterceptorProvider.get());
  }

  @InjectedFieldSignature("io.deephaven.server.runner.DeephavenApiServerTestBase.executionContext")
  public static void injectExecutionContext(DeephavenApiServerTestBase instance,
      ExecutionContext executionContext) {
    instance.executionContext = executionContext;
  }

  @InjectedFieldSignature("io.deephaven.server.runner.DeephavenApiServerTestBase.server")
  public static void injectServer(DeephavenApiServerTestBase instance, DeephavenApiServer server) {
    instance.server = server;
  }

  @InjectedFieldSignature("io.deephaven.server.runner.DeephavenApiServerTestBase.scheduler")
  public static void injectScheduler(DeephavenApiServerTestBase instance,
      Scheduler.DelegatingImpl scheduler) {
    instance.scheduler = scheduler;
  }

  @InjectedFieldSignature("io.deephaven.server.runner.DeephavenApiServerTestBase.scriptSessionProvider")
  public static void injectScriptSessionProvider(DeephavenApiServerTestBase instance,
      Provider<ScriptSession> scriptSessionProvider) {
    instance.scriptSessionProvider = scriptSessionProvider;
  }

  @InjectedFieldSignature("io.deephaven.server.runner.DeephavenApiServerTestBase.managedChannelBuilderProvider")
  public static void injectManagedChannelBuilderProvider(DeephavenApiServerTestBase instance,
      Provider<ManagedChannelBuilder<?>> managedChannelBuilderProvider) {
    instance.managedChannelBuilderProvider = managedChannelBuilderProvider;
  }

  @InjectedFieldSignature("io.deephaven.server.runner.DeephavenApiServerTestBase.serverStateInterceptor")
  public static void injectServerStateInterceptor(DeephavenApiServerTestBase instance,
      RpcServerStateInterceptor serverStateInterceptor) {
    instance.serverStateInterceptor = serverStateInterceptor;
  }
}
