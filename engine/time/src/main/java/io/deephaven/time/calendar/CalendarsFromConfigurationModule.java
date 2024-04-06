//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.HashSet;
import java.util.Set;

/**
 * Provides the {@link BusinessCalendar business calendars} from {@link Calendars#calendarsFromConfiguration()}.
 */
@Module
public interface CalendarsFromConfigurationModule {

    @Provides
    @ElementsIntoSet
    static Set<BusinessCalendar> providesCalendarsFromConfiguration() {
        return new HashSet<>(Calendars.calendarsFromConfiguration());
    }
}
