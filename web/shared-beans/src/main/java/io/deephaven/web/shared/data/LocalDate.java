package io.deephaven.web.shared.data;

import java.io.Serializable;

/**
 * A simple container for serializing LocalDate values. This should be better for serialization than
 * java.time.LocalDate since we use bytes for month and day, and is compatible with GWT (java.time
 * is not available in GWT).
 */
public class LocalDate implements Serializable {
    private int year;
    private byte monthValue, dayOfMonth;

    public LocalDate(int year, byte monthValue, byte dayOfMonth) {
        this.year = year;
        this.monthValue = monthValue;
        this.dayOfMonth = dayOfMonth;
    }

    public LocalDate() {}

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public byte getMonthValue() {
        return monthValue;
    }

    public void setMonthValue(byte monthValue) {
        this.monthValue = monthValue;
    }

    public byte getDayOfMonth() {
        return dayOfMonth;
    }

    public void setDayOfMonth(byte dayOfMonth) {
        this.dayOfMonth = dayOfMonth;
    }
}
