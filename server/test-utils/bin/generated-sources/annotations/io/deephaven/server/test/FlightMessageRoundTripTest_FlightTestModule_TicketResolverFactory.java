package io.deephaven.server.test;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.session.TicketResolver;
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
    "rawtypes",
    "KotlinInternal",
    "KotlinInternalInJava",
    "cast"
})
public final class FlightMessageRoundTripTest_FlightTestModule_TicketResolverFactory implements Factory<TicketResolver> {
  private final FlightMessageRoundTripTest.FlightTestModule module;

  private final Provider<ScopeTicketResolver> resolverProvider;

  public FlightMessageRoundTripTest_FlightTestModule_TicketResolverFactory(
      FlightMessageRoundTripTest.FlightTestModule module,
      Provider<ScopeTicketResolver> resolverProvider) {
    this.module = module;
    this.resolverProvider = resolverProvider;
  }

  @Override
  public TicketResolver get() {
    return ticketResolver(module, resolverProvider.get());
  }

  public static FlightMessageRoundTripTest_FlightTestModule_TicketResolverFactory create(
      FlightMessageRoundTripTest.FlightTestModule module,
      Provider<ScopeTicketResolver> resolverProvider) {
    return new FlightMessageRoundTripTest_FlightTestModule_TicketResolverFactory(module, resolverProvider);
  }

  public static TicketResolver ticketResolver(FlightMessageRoundTripTest.FlightTestModule instance,
      ScopeTicketResolver resolver) {
    return Preconditions.checkNotNullFromProvides(instance.ticketResolver(resolver));
  }
}
