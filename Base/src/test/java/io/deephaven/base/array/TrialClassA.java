/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.array;

import io.deephaven.base.Copyable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class TrialClassA implements Externalizable, Copyable<TrialClassA> {
    private static final long serialVersionUID = -9384756L;
    private static final int versionId = 0;
    double double1;
    int int1;
    long long1;

    public TrialClassA() {
        this.double1 = Double.NaN;
        this.int1 = Integer.MIN_VALUE;
        this.long1 = Long.MIN_VALUE;
    }

    public TrialClassA(double double1, int int1, long long1) {
        this.double1 = double1;
        this.int1 = int1;
        this.long1 = long1;
    }

    // public static TrialClassA newInstance() {
    // return new TrialClassA();
    // }

    @Override
    public synchronized void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(versionId);
        out.writeDouble(double1);
        out.writeInt(int1);
        out.writeLong(long1);
    }

    @Override
    public synchronized void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int localVersionId = in.readInt();
        double1 = in.readDouble();
        int1 = in.readInt();
        long1 = in.readLong();
    }


    public TrialClassA deepCopy() {
        return new TrialClassA(double1, int1, long1);
    }

    public double getDouble1() {
        return double1;
    }

    public void setDouble1(double double1) {
        this.double1 = double1;
    }

    public int getInt1() {
        return int1;
    }

    public void setInt1(int int1) {
        this.int1 = int1;
    }

    public long getLong1() {
        return long1;
    }

    public void setLong1(long long1) {
        this.long1 = long1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof TrialClassA))
            return false;

        TrialClassA that = (TrialClassA) o;

        if (Double.compare(that.double1, double1) != 0)
            return false;
        if (int1 != that.int1)
            return false;
        if (long1 != that.long1)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = double1 != +0.0d ? Double.doubleToLongBits(double1) : 0L;
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + int1;
        result = 31 * result + (int) (long1 ^ (long1 >>> 32));
        return result;
    }

    @Override
    public void copyValues(TrialClassA other) {
        this.double1 = other.double1;
        this.int1 = other.int1;
        this.long1 = other.long1;
    }

    @Override
    public TrialClassA clone() {
        return deepCopy();
    }

    @Override
    public TrialClassA safeClone() {
        return clone();
    }

    @Override
    public String toString() {
        return "TrialClassA: " + double1 + " " + int1 + " " + long1;
    }

    public static TrialClassA makeNull() {
        return new TrialClassA(Double.NaN, Integer.MIN_VALUE, Long.MIN_VALUE);
    }

    ////////////////////////////////////////////////////////////////////////////////////

    public static FastArray.WriteExternalFunction<TrialClassA> getWriter() {
        return new TestClassWriter();
    }

    public static FastArray.ReadExternalFunction<TrialClassA> getReader() {
        return new TestClassReader();
    }

    private static class TestClassWriter implements FastArray.WriteExternalFunction<TrialClassA> {

        @Override
        public void writeExternal(ObjectOutput out, TrialClassA item) throws IOException {
            out.writeObject(item);
        }
    }

    private static class TestClassReader implements FastArray.ReadExternalFunction<TrialClassA> {
        @Override
        public void readExternal(ObjectInput in, TrialClassA item) throws IOException, ClassNotFoundException {
            TrialClassA readObject = (TrialClassA) in.readObject();
            item.copyValues(readObject);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////
    //
    // public static FastArrayF.WriteExternalFunction<TrialClassA> getWriterF() {
    // return new TestClassWriterF();
    // }
    //
    // public static FastArrayF.ReadExternalFunction<TrialClassA> getReaderF() {
    // return new TestClassReaderF();
    // }
    //
    // private static class TestClassWriterF implements FastArrayF.WriteExternalFunction<TrialClassA> {
    //
    // @Override
    // public void writeExternal(ObjectOutput out, TrialClassA item) throws IOException {
    // out.writeObject(item);
    // }
    // }
    //
    // private static class TestClassReaderF implements FastArrayF.ReadExternalFunction<TrialClassA> {
    // @Override
    // public void readExternal(ObjectInput in, TrialClassA item) throws IOException, ClassNotFoundException {
    // TrialClassA readObject = (TrialClassA) in.readObject();
    // item.copyValues(readObject);
    // }
    // }
    //
    // ////////////////////////////////////////////////////////////////////////////////////
    //
    // public static FastArrayFactoryInterface<TrialClassA> getFactory() {
    // return new TestClassFactory();
    // }
    //
    // private static class TestClassFactory implements FastArrayFactoryInterface<TrialClassA> {
    //
    // @Override
    // public TrialClassA newInstance() {
    // return new TrialClassA(Double.NaN, Integer.MIN_VALUE, Long.MIN_VALUE);
    // }
    //
    // @Override
    // public TrialClassA[] newInstanceArray(int length) {
    // TrialClassA[] result = new TrialClassA[length];
    // for (int i = 0; i < result.length; i++) {
    // result[i] = newInstance();
    // }
    // return result;
    // }
    // }
}
