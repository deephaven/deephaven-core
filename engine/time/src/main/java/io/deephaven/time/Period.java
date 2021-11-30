/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time;

import java.io.*;

public class Period implements Comparable<Period>, Serializable {

    private String periodString;

    private transient org.joda.time.Period period = null;

    public Period() {
        // for serialization...
    }

    public Period(String periodString) {
        char ret[] = periodString.toCharArray();

        for (int i = 0; i < ret.length; i++) {
            if (Character.toUpperCase(ret[i]) != 'T') {
                ret[i] = Character.toLowerCase(ret[i]);
            } else {
                ret[i] = Character.toUpperCase(ret[i]);
            }
        }

        this.periodString = new String(ret).intern();

        initPeriod();
    }

    private void initPeriod() {
        period = new org.joda.time.Period("P" + (isPositive() ? periodString : periodString.substring(1)));
    }

    public org.joda.time.Period getJodaPeriod() {
        return period;
    }

    public boolean isPositive() {
        return periodString.charAt(0) != '-';
    }

    public boolean equals(Object period) {
        return periodString.equals(((Period) period).periodString);
    }

    public int hashCode() {
        return periodString.hashCode();
    }

    public int compareTo(Period dateTime) {
        return periodString.compareTo(dateTime.periodString);
    }

    public String toString() {
        return periodString;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        initPeriod();
    }
}
