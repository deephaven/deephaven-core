package io.deephaven.web.shared.data;

import java.io.Serializable;

/**
 * A simple container for serializing local time values. This should be better for serialization than
 * java.time.LocalTime since we use bytes for hour, minute and second, and is compatible with GWT (java.time is not
 * available in GWT).
 */
public class LocalTime implements Serializable {
    private byte hour, minute, second;
    private int nano;

    public LocalTime(byte hour, byte minute, byte second, int nano) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.nano = nano;
    }

    public LocalTime() {}

    public byte getHour() {
        return hour;
    }

    public void setHour(byte hour) {
        this.hour = hour;
    }

    public byte getMinute() {
        return minute;
    }

    public void setMinute(byte minute) {
        this.minute = minute;
    }

    public byte getSecond() {
        return second;
    }

    public void setSecond(byte second) {
        this.second = second;
    }

    public int getNano() {
        return nano;
    }

    public void setNano(int nano) {
        this.nano = nano;
    }
}
