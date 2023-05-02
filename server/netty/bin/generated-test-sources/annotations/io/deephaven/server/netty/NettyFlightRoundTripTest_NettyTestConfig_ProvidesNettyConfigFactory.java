package io.deephaven.server.netty;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import javax.annotation.processing.Generated;

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
public final class NettyFlightRoundTripTest_NettyTestConfig_ProvidesNettyConfigFactory implements Factory<NettyConfig> {
  @Override
  public NettyConfig get() {
    return providesNettyConfig();
  }

  public static NettyFlightRoundTripTest_NettyTestConfig_ProvidesNettyConfigFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static NettyConfig providesNettyConfig() {
    return Preconditions.checkNotNullFromProvides(NettyFlightRoundTripTest.NettyTestConfig.providesNettyConfig());
  }

  private static final class InstanceHolder {
    private static final NettyFlightRoundTripTest_NettyTestConfig_ProvidesNettyConfigFactory INSTANCE = new NettyFlightRoundTripTest_NettyTestConfig_ProvidesNettyConfigFactory();
  }
}
