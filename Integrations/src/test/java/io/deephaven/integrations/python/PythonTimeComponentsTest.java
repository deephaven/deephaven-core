//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static io.deephaven.integrations.python.PythonTimeComponents.getDurationComponents;
import static io.deephaven.integrations.python.PythonTimeComponents.getInstantComponents;
import static io.deephaven.integrations.python.PythonTimeComponents.getLocalDateComponents;
import static io.deephaven.integrations.python.PythonTimeComponents.getLocalTimeComponents;
import static io.deephaven.integrations.python.PythonTimeComponents.getPeriodComponents;

public class PythonTimeComponentsTest {

    private static final LocalTime TIME_1234 = LocalTime.of(1, 2, 3, 4);
    private static final Instant INSTANT_42_43 = Instant.ofEpochSecond(42, 43);
    private static final LocalDate DATE_2023_FEB_7 = LocalDate.of(2023, Month.FEBRUARY, 7);
    private static final Duration DURATION_101_102 = Duration.ofSeconds(101, 102);
    private static final Period PERIOD_3_4_5 = Period.of(3, 4, 5);

    @Test
    public void getLocalTimeComponentsFromLocalTime() {
        Assert.assertArrayEquals(new int[] {1, 2, 3, 4}, getLocalTimeComponents(TIME_1234));
    }

    @Test
    public void getLocalTimeComponentsFromZonedDateTime() {
        Assert.assertArrayEquals(new int[] {1, 2, 3, 4},
                getLocalTimeComponents(ZonedDateTime.of(DATE_2023_FEB_7, TIME_1234, ZoneId.systemDefault())));
    }

    @Test
    public void getInstantComponentsFromInstant() {
        Assert.assertArrayEquals(new long[] {42, 43}, getInstantComponents(INSTANT_42_43));
    }

    @Test
    public void getInstantComponentsFromZonedDateTime() {
        Assert.assertArrayEquals(new long[] {42, 43},
                getInstantComponents(INSTANT_42_43.atZone(ZoneId.systemDefault())));
    }

    @Test
    public void getDurationComponentsFromDuration() {
        Assert.assertArrayEquals(new long[] {101, 102}, getDurationComponents(DURATION_101_102));
    }

    @Test
    public void getPeriodComponentsFromPeriod() {
        Assert.assertArrayEquals(new int[] {3, 4, 5}, getPeriodComponents(PERIOD_3_4_5));
    }

    @Test
    public void getLocalDateComponentsFromLocalDate() {
        Assert.assertArrayEquals(new int[] {2023, 2, 7}, getLocalDateComponents(DATE_2023_FEB_7));
    }

    @Test
    public void getLocalDateComponentsFromLocalDateFromZonedDateTime() {
        Assert.assertArrayEquals(new int[] {2023, 2, 7},
                getLocalDateComponents(ZonedDateTime.of(DATE_2023_FEB_7, TIME_1234, ZoneId.systemDefault())));
    }
}
