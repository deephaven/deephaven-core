/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * A time period that can be expressed in an easily human-readable format.
 */
public class Period implements Comparable<Period>, Serializable {
    //TODO: test coverage

    private String periodString;

    private transient boolean isPositive;
    private transient java.time.Duration duration = null;

    /**
     * Constructor for serialization / deserialization.  This should not be used.
     */
    public Period() {
        // for serialization...
    }

    /**
     * Create a new Period initialized to the provided period string.
     *
     * @param s string in the form of "nYnMnWnDTnHnMnS", with n being numeric values, e.g. 1W for one week, T1M for
     *          one minute, 1WT1H for one week plus one hour.
     */
    //TODO: how are subseconds done?
    //TODO: null annotations
    public Period(String s) {
        char[] ret = s.toCharArray();

        for (int i = 0; i < ret.length; i++) {
            if (Character.toUpperCase(ret[i]) != 'T') {
                ret[i] = Character.toLowerCase(ret[i]);
            } else {
                ret[i] = Character.toUpperCase(ret[i]);
            }
        }

        this.periodString = new String(ret).intern();

        init();
    }

    private void init() {
        isPositive = periodString.charAt(0) != '-'
        duration = java.time.Duration.parse("P" + (isPositive() ? periodString : periodString.substring(1)));
    }

    //TODO: document
    //TODO: remove?
    //TODO: null annotations
    /**
     * Get the {@link java.time.Duration} associated with this period.  The {@link java.time.Duration}
     * @return
     */
    java.time.Duration getDuration() {
        return duration;
    }

    //TOOD: clean up all plus / minus math
    //TODO: get rid of the isPositive and just let Duration do it.
    /**
     * Determines if the time period is positive.
     *
     * @return true if the period is positive, and false otherwise.
     */
    public boolean isPositive() {
        return isPositive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Period period1 = (Period) o;
        return periodString.equals(period1.periodString);
    }

    @Override
    public int hashCode() {
        return periodString.hashCode();
    }

    @Override
    public int compareTo(Period dateTime) {
        return periodString.compareTo(dateTime.periodString);
    }

    @Override
    public String toString() {
        return periodString;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        init();
    }
}
