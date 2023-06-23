package io.deephaven.client.impl;

import dagger.internal.DaggerGenerated;
import dagger.internal.Preconditions;
import io.deephaven.client.SessionImplModule_SessionFactory;
import io.deephaven.proto.DeephavenChannelImpl;
import io.grpc.ManagedChannel;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.processing.Generated;
import org.apache.arrow.memory.BufferAllocator;

@DaggerGenerated
@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class DaggerDeephavenFlightRoot {
  private DaggerDeephavenFlightRoot() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static DeephavenFlightRoot create() {
    return new Builder().build();
  }

  public static final class Builder {
    private Builder() {
    }

    public DeephavenFlightRoot build() {
      return new DeephavenFlightRootImpl();
    }
  }

  private static final class FlightSubcomponentBuilder implements FlightSubcomponent.Builder {
    private final DeephavenFlightRootImpl deephavenFlightRootImpl;

    private BufferAllocator allocator;

    private String authenticationTypeAndValue;

    private ManagedChannel managedChannel;

    private ScheduledExecutorService scheduler;

    private FlightSubcomponentBuilder(DeephavenFlightRootImpl deephavenFlightRootImpl) {
      this.deephavenFlightRootImpl = deephavenFlightRootImpl;
    }

    @Override
    public FlightSubcomponentBuilder allocator(BufferAllocator arg0) {
      this.allocator = Preconditions.checkNotNull(arg0);
      return this;
    }

    @Override
    public FlightSubcomponentBuilder authenticationTypeAndValue(String arg0) {
      this.authenticationTypeAndValue = arg0;
      return this;
    }

    @Override
    public FlightSubcomponentBuilder managedChannel(ManagedChannel arg0) {
      this.managedChannel = Preconditions.checkNotNull(arg0);
      return this;
    }

    @Override
    public FlightSubcomponentBuilder scheduler(ScheduledExecutorService arg0) {
      this.scheduler = Preconditions.checkNotNull(arg0);
      return this;
    }

    @Override
    public FlightSubcomponent build() {
      Preconditions.checkBuilderRequirement(allocator, BufferAllocator.class);
      Preconditions.checkBuilderRequirement(managedChannel, ManagedChannel.class);
      Preconditions.checkBuilderRequirement(scheduler, ScheduledExecutorService.class);
      return new FlightSubcomponentImpl(deephavenFlightRootImpl, allocator, authenticationTypeAndValue, managedChannel, scheduler);
    }
  }

  private static final class FlightSubcomponentImpl implements FlightSubcomponent {
    private final ManagedChannel managedChannel;

    private final ScheduledExecutorService scheduler;

    private final String authenticationTypeAndValue;

    private final BufferAllocator allocator;

    private final DeephavenFlightRootImpl deephavenFlightRootImpl;

    private final FlightSubcomponentImpl flightSubcomponentImpl = this;

    private FlightSubcomponentImpl(DeephavenFlightRootImpl deephavenFlightRootImpl,
        BufferAllocator allocatorParam, String authenticationTypeAndValueParam,
        ManagedChannel managedChannelParam, ScheduledExecutorService schedulerParam) {
      this.deephavenFlightRootImpl = deephavenFlightRootImpl;
      this.managedChannel = managedChannelParam;
      this.scheduler = schedulerParam;
      this.authenticationTypeAndValue = authenticationTypeAndValueParam;
      this.allocator = allocatorParam;

    }

    private DeephavenChannelImpl deephavenChannelImpl() {
      return new DeephavenChannelImpl(managedChannel);
    }

    private SessionImpl sessionImpl() {
      return SessionImplModule_SessionFactory.session(deephavenChannelImpl(), scheduler, authenticationTypeAndValue);
    }

    @Override
    public FlightSession newFlightSession() {
      return FlightSessionModule_NewFlightSessionFactory.newFlightSession(sessionImpl(), allocator, managedChannel);
    }
  }

  private static final class DeephavenFlightRootImpl implements DeephavenFlightRoot {
    private final DeephavenFlightRootImpl deephavenFlightRootImpl = this;

    private DeephavenFlightRootImpl() {


    }

    @Override
    public FlightSubcomponent.Builder factoryBuilder() {
      return new FlightSubcomponentBuilder(deephavenFlightRootImpl);
    }
  }
}
