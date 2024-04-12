package io.deephaven.client;

import dagger.internal.DaggerGenerated;
import dagger.internal.Preconditions;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.proto.DeephavenChannelImpl;
import io.grpc.ManagedChannel;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.processing.Generated;

@DaggerGenerated
@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class DaggerDeephavenSessionRoot {
  private DaggerDeephavenSessionRoot() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static DeephavenSessionRoot create() {
    return new Builder().build();
  }

  public static final class Builder {
    private Builder() {
    }

    public DeephavenSessionRoot build() {
      return new DeephavenSessionRootImpl();
    }
  }

  private static final class SessionSubcomponentBuilder implements SessionSubcomponent.Builder {
    private final DeephavenSessionRootImpl deephavenSessionRootImpl;

    private ManagedChannel managedChannel;

    private ScheduledExecutorService scheduler;

    private String authenticationTypeAndValue;

    private SessionSubcomponentBuilder(DeephavenSessionRootImpl deephavenSessionRootImpl) {
      this.deephavenSessionRootImpl = deephavenSessionRootImpl;
    }

    @Override
    public SessionSubcomponentBuilder managedChannel(ManagedChannel channel) {
      this.managedChannel = Preconditions.checkNotNull(channel);
      return this;
    }

    @Override
    public SessionSubcomponentBuilder scheduler(ScheduledExecutorService scheduler) {
      this.scheduler = Preconditions.checkNotNull(scheduler);
      return this;
    }

    @Override
    public SessionSubcomponentBuilder authenticationTypeAndValue(
        String authenticationTypeAndValue) {
      this.authenticationTypeAndValue = authenticationTypeAndValue;
      return this;
    }

    @Override
    public SessionSubcomponent build() {
      Preconditions.checkBuilderRequirement(managedChannel, ManagedChannel.class);
      Preconditions.checkBuilderRequirement(scheduler, ScheduledExecutorService.class);
      return new SessionSubcomponentImpl(deephavenSessionRootImpl, managedChannel, scheduler, authenticationTypeAndValue);
    }
  }

  private static final class SessionSubcomponentImpl implements SessionSubcomponent {
    private final ManagedChannel managedChannel;

    private final ScheduledExecutorService scheduler;

    private final String authenticationTypeAndValue;

    private final DeephavenSessionRootImpl deephavenSessionRootImpl;

    private final SessionSubcomponentImpl sessionSubcomponentImpl = this;

    private SessionSubcomponentImpl(DeephavenSessionRootImpl deephavenSessionRootImpl,
        ManagedChannel managedChannelParam, ScheduledExecutorService schedulerParam,
        String authenticationTypeAndValueParam) {
      this.deephavenSessionRootImpl = deephavenSessionRootImpl;
      this.managedChannel = managedChannelParam;
      this.scheduler = schedulerParam;
      this.authenticationTypeAndValue = authenticationTypeAndValueParam;

    }

    private DeephavenChannelImpl deephavenChannelImpl() {
      return new DeephavenChannelImpl(managedChannel);
    }

    @Override
    public SessionImpl newSession() {
      return SessionImplModule_SessionFactory.session(deephavenChannelImpl(), scheduler, authenticationTypeAndValue);
    }
  }

  private static final class DeephavenSessionRootImpl implements DeephavenSessionRoot {
    private final DeephavenSessionRootImpl deephavenSessionRootImpl = this;

    private DeephavenSessionRootImpl() {


    }

    @Override
    public SessionSubcomponent.Builder factoryBuilder() {
      return new SessionSubcomponentBuilder(deephavenSessionRootImpl);
    }
  }
}
