package io.deephaven.time.calendar;

import dagger.internal.DaggerGenerated;
import dagger.internal.Factory;
import dagger.internal.Preconditions;
import dagger.internal.QualifierMetadata;
import dagger.internal.ScopeMetadata;
import java.util.Set;
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
    "rawtypes",
    "KotlinInternal",
    "KotlinInternalInJava",
    "cast"
})
public final class CalendarsFromConfigurationModule_ProvidesCalendarsFromConfigurationFactory implements Factory<Set<BusinessCalendar>> {
  @Override
  public Set<BusinessCalendar> get() {
    return providesCalendarsFromConfiguration();
  }

  public static CalendarsFromConfigurationModule_ProvidesCalendarsFromConfigurationFactory create(
      ) {
    return InstanceHolder.INSTANCE;
  }

  public static Set<BusinessCalendar> providesCalendarsFromConfiguration() {
    return Preconditions.checkNotNullFromProvides(CalendarsFromConfigurationModule.providesCalendarsFromConfiguration());
  }

  private static final class InstanceHolder {
    private static final CalendarsFromConfigurationModule_ProvidesCalendarsFromConfigurationFactory INSTANCE = new CalendarsFromConfigurationModule_ProvidesCalendarsFromConfigurationFactory();
  }
}
