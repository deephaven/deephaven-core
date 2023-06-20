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
public final class DaggerDeephavenBarrageRoot {
  private DaggerDeephavenBarrageRoot() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static DeephavenBarrageRoot create() {
    return new Builder().build();
  }

  public static final class Builder {
    private Builder() {
    }

    public DeephavenBarrageRoot build() {
      return new DeephavenBarrageRootImpl();
    }
  }

  private static final class BarrageSubcomponentBuilder implements BarrageSubcomponent.Builder {
    private final DeephavenBarrageRootImpl deephavenBarrageRootImpl;

    private BufferAllocator allocator;

    private String authenticationTypeAndValue;

    private ManagedChannel managedChannel;

    private ScheduledExecutorService scheduler;

    private BarrageSubcomponentBuilder(DeephavenBarrageRootImpl deephavenBarrageRootImpl) {
      this.deephavenBarrageRootImpl = deephavenBarrageRootImpl;
    }

    @Override
    public BarrageSubcomponentBuilder allocator(BufferAllocator bufferAllocator) {
      this.allocator = Preconditions.checkNotNull(bufferAllocator);
      return this;
    }

    @Override
    public BarrageSubcomponentBuilder authenticationTypeAndValue(
        String authenticationTypeAndValue) {
      this.authenticationTypeAndValue = authenticationTypeAndValue;
      return this;
    }

    @Override
    public BarrageSubcomponentBuilder managedChannel(ManagedChannel channel) {
      this.managedChannel = Preconditions.checkNotNull(channel);
      return this;
    }

    @Override
    public BarrageSubcomponentBuilder scheduler(ScheduledExecutorService scheduler) {
      this.scheduler = Preconditions.checkNotNull(scheduler);
      return this;
    }

    @Override
    public BarrageSubcomponent build() {
      Preconditions.checkBuilderRequirement(allocator, BufferAllocator.class);
      Preconditions.checkBuilderRequirement(managedChannel, ManagedChannel.class);
      Preconditions.checkBuilderRequirement(scheduler, ScheduledExecutorService.class);
      return new BarrageSubcomponentImpl(deephavenBarrageRootImpl, allocator, authenticationTypeAndValue, managedChannel, scheduler);
    }
  }

  private static final class BarrageSubcomponentImpl implements BarrageSubcomponent {
    private final ManagedChannel managedChannel;

    private final ScheduledExecutorService scheduler;

    private final String authenticationTypeAndValue;

    private final BufferAllocator allocator;

    private final DeephavenBarrageRootImpl deephavenBarrageRootImpl;

    private final BarrageSubcomponentImpl barrageSubcomponentImpl = this;

    private BarrageSubcomponentImpl(DeephavenBarrageRootImpl deephavenBarrageRootImpl,
        BufferAllocator allocatorParam, String authenticationTypeAndValueParam,
        ManagedChannel managedChannelParam, ScheduledExecutorService schedulerParam) {
      this.deephavenBarrageRootImpl = deephavenBarrageRootImpl;
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
    public BarrageSession newBarrageSession() {
      return BarrageSessionModule_NewDeephavenClientSessionFactory.newDeephavenClientSession(sessionImpl(), allocator, managedChannel);
    }
  }

  private static final class DeephavenBarrageRootImpl implements DeephavenBarrageRoot {
    private final DeephavenBarrageRootImpl deephavenBarrageRootImpl = this;

    private DeephavenBarrageRootImpl() {


    }

    @Override
    public BarrageSubcomponent.Builder factoryBuilder() {
      return new BarrageSubcomponentBuilder(deephavenBarrageRootImpl);
    }
  }
}
