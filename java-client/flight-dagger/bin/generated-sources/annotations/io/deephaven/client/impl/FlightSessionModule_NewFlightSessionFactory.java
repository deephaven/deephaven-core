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
public final class FlightSessionModule_NewFlightSessionFactory implements Factory<FlightSession> {
  private final Provider<SessionImpl> sessionProvider;

  private final Provider<BufferAllocator> allocatorProvider;

  private final Provider<ManagedChannel> managedChannelProvider;

  public FlightSessionModule_NewFlightSessionFactory(Provider<SessionImpl> sessionProvider,
      Provider<BufferAllocator> allocatorProvider,
      Provider<ManagedChannel> managedChannelProvider) {
    this.sessionProvider = sessionProvider;
    this.allocatorProvider = allocatorProvider;
    this.managedChannelProvider = managedChannelProvider;
  }

  @Override
  public FlightSession get() {
    return newFlightSession(sessionProvider.get(), allocatorProvider.get(), managedChannelProvider.get());
  }

  public static FlightSessionModule_NewFlightSessionFactory create(
      Provider<SessionImpl> sessionProvider, Provider<BufferAllocator> allocatorProvider,
      Provider<ManagedChannel> managedChannelProvider) {
    return new FlightSessionModule_NewFlightSessionFactory(sessionProvider, allocatorProvider, managedChannelProvider);
  }

  public static FlightSession newFlightSession(SessionImpl session, BufferAllocator allocator,
      ManagedChannel managedChannel) {
    return Preconditions.checkNotNullFromProvides(FlightSessionModule.newFlightSession(session, allocator, managedChannel));
  }
}
