//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeTableTest {

    @Test
    void ofDuration() {
        final TimeTable t1 = TimeTable.of(Duration.ofSeconds(1));
        assertThat(t1.interval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(t1.startTime()).isEmpty();
        assertThat(t1.blinkTable()).isFalse();
        assertThat(t1.clock()).isEqualTo(ClockSystem.INSTANCE);
    }

    @Test
    void ofDurationBlink() {
        final TimeTable t1 = TimeTable.builder().interval(Duration.ofSeconds(1)).blinkTable(true).build();
        assertThat(t1.interval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(t1.startTime()).isEmpty();
        assertThat(t1.blinkTable()).isTrue();
        assertThat(t1.clock()).isEqualTo(ClockSystem.INSTANCE);
    }

    @Test
    void ofDurationNotEquals() {
        final TimeTable t1 = TimeTable.of(Duration.ofSeconds(1));
        final TimeTable t2 = TimeTable.of(Duration.ofSeconds(1));
        assertThat(t1).isNotEqualTo(t2);
    }

    @Test
    void ofDurationInstantEquals() {
        final TimeTable t1 = TimeTable.of(Duration.ofSeconds(1), Instant.ofEpochSecond(37));
        final TimeTable t2 = TimeTable.of(Duration.ofSeconds(1), Instant.ofEpochSecond(37));
        assertThat(t1).isEqualTo(t2);
    }

    @Test
    void ofDurationInstantBuilderNotEquals() {
        final TimeTable t1 = TimeTable.builder()
                .interval(Duration.ofSeconds(1))
                .startTime(Instant.ofEpochSecond(37))
                .build();
        final TimeTable t2 = TimeTable.builder()
                .interval(Duration.ofSeconds(1))
                .startTime(Instant.ofEpochSecond(37))
                .build();
        assertThat(t1).isNotEqualTo(t2);
    }

    @Test
    void ofDurationInstantCrossBuilderNotEquals() {
        final TimeTable t1 = TimeTable.of(Duration.ofSeconds(1), Instant.ofEpochSecond(37));
        final TimeTable t2 = TimeTable.builder()
                .interval(Duration.ofSeconds(1))
                .startTime(Instant.ofEpochSecond(37))
                .build();
        assertThat(t1).isNotEqualTo(t2);
    }

    @Test
    void ofDurationInstantIdEquals() {
        final Object myId = new Object();
        final TimeTable t1 = TimeTable.builder()
                .interval(Duration.ofSeconds(1))
                .startTime(Instant.ofEpochSecond(37))
                .id(myId)
                .build();
        final TimeTable t2 = TimeTable.builder()
                .interval(Duration.ofSeconds(1))
                .startTime(Instant.ofEpochSecond(37))
                .id(myId)
                .build();
        assertThat(t1).isEqualTo(t2);
    }

    @Test
    void ofInstantIdEquals() {
        final Object myId = new Object();
        final TimeTable t1 = TimeTable.builder()
                .interval(Duration.ofSeconds(1))
                .startTime(Instant.ofEpochSecond(37))
                .id(myId)
                .build();
        final TimeTable t2 = TimeTable.builder()
                .interval(Duration.ofSeconds(2))
                .startTime(Instant.ofEpochSecond(37))
                .id(myId)
                .build();
        assertThat(t1).isNotEqualTo(t2);
    }

    @Test
    void ofDurationIdEquals() {
        final Object myId = new Object();
        final TimeTable t1 = TimeTable.builder()
                .interval(Duration.ofSeconds(1))
                .startTime(Instant.ofEpochSecond(37))
                .id(myId)
                .build();
        final TimeTable t2 = TimeTable.builder()
                .interval(Duration.ofSeconds(1))
                .startTime(Instant.ofEpochSecond(38))
                .id(myId)
                .build();
        assertThat(t1).isNotEqualTo(t2);
    }
}
