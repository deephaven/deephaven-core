package io.deephaven.client.impl;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.grpc.ManagedChannel;
import javax.annotation.processing.Generated;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;

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
public final class BarrageSessionModule_NewDeephavenClientSessionFactory implements Factory<BarrageSession> {
  private final Provider<SessionImpl> sessionProvider;

  private final Provider<BufferAllocator> allocatorProvider;

  private final Provider<ManagedChannel> managedChannelProvider;

  public BarrageSessionModule_NewDeephavenClientSessionFactory(
      Provider<SessionImpl> sessionProvider, Provider<BufferAllocator> allocatorProvider,
      Provider<ManagedChannel> managedChannelProvider) {
    this.sessionProvider = sessionProvider;
    this.allocatorProvider = allocatorProvider;
    this.managedChannelProvider = managedChannelProvider;
  }

  @Override
  public BarrageSession get() {
    return newDeephavenClientSession(sessionProvider.get(), allocatorProvider.get(), managedChannelProvider.get());
  }

  public static BarrageSessionModule_NewDeephavenClientSessionFactory create(
      Provider<SessionImpl> sessionProvider, Provider<BufferAllocator> allocatorProvider,
      Provider<ManagedChannel> managedChannelProvider) {
    return new BarrageSessionModule_NewDeephavenClientSessionFactory(sessionProvider, allocatorProvider, managedChannelProvider);
  }

  public static BarrageSession newDeephavenClientSession(SessionImpl session,
      BufferAllocator allocator, ManagedChannel managedChannel) {
    return Preconditions.checkNotNullFromProvides(BarrageSessionModule.newDeephavenClientSession(session, allocator, managedChannel));
  }
}
