package io.deephaven.server.jetty;

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
public final class JettyFlightRoundTripTest_JettyTestConfig_ProvidesJettyConfigFactory implements Factory<JettyConfig> {
  @Override
  public JettyConfig get() {
    return providesJettyConfig();
  }

  public static JettyFlightRoundTripTest_JettyTestConfig_ProvidesJettyConfigFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static JettyConfig providesJettyConfig() {
    return Preconditions.checkNotNullFromProvides(JettyFlightRoundTripTest.JettyTestConfig.providesJettyConfig());
  }

  private static final class InstanceHolder {
    private static final JettyFlightRoundTripTest_JettyTestConfig_ProvidesJettyConfigFactory INSTANCE = new JettyFlightRoundTripTest_JettyTestConfig_ProvidesJettyConfigFactory();
  }
}
