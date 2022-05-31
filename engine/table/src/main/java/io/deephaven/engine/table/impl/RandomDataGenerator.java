/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import java.util.Random;
import java.util.Set;
import java.util.HashSet;

public class RandomDataGenerator {
    public static char[] getCharArray(int size) {
        Random random = new Random(0);
        Set<Character> seen = new HashSet<Character>();
        char data[] = new char[size];
        for (int i = 0; i < data.length; i++) {
            do {
                data[i] = (char) random.nextInt();
            } while (seen.contains(data[i]));
            seen.add(data[i]);
        }
        return data;
    }

    public static float[] getFloatArray(int size) {
        Random random = new Random(0);
        float data[] = new float[size];
        Set<Float> seen = new HashSet<Float>();
        for (int i = 0; i < data.length; i++) {
            do {
                data[i] = random.nextFloat() * 10000;
            } while (seen.contains(data[i]));
            seen.add(data[i]);
        }
        return data;
    }

    public static double[] getDoubleArray(int size) {
        Random random = new Random(0);
        double data[] = new double[size];
        Set<Double> seen = new HashSet<Double>();
        for (int i = 0; i < data.length; i++) {
            do {
                data[i] = random.nextDouble() * 10000;
            } while (seen.contains(data[i]));
            seen.add(data[i]);
        }
        return data;
    }

    public static int[] getIntArray(int size) {
        Random random = new Random(0);
        int data[] = new int[size];
        Set<Integer> seen = new HashSet<Integer>();
        for (int i = 0; i < data.length; i++) {
            do {
                data[i] = random.nextInt();
            } while (seen.contains(data[i]));
            seen.add(data[i]);
        }
        return data;
    }

    public static long[] getLongArray(int size) {
        Random random = new Random(0);
        long data[] = new long[size];
        Set<Long> seen = new HashSet<Long>();
        for (int i = 0; i < data.length; i++) {
            do {
                data[i] = random.nextLong();
            } while (seen.contains(data[i]));
            seen.add(data[i]);
        }
        return data;
    }

    public static Boolean[] getBooleanArray() {
        return new Boolean[] {false, true};
    }

    public static byte[] getByteArray(int size) {
        Random random = new Random(0);
        byte data[] = new byte[size];
        Set<Byte> seen = new HashSet<Byte>();
        for (int i = 0; i < data.length; i++) {
            do {
                data[i] = (byte) random.nextInt();
            } while (seen.contains(data[i]));
            seen.add(data[i]);
        }
        return data;
    }

    public static short[] getShortArray(int size) {
        Random random = new Random(0);
        short data[] = new short[size];
        Set<Short> seen = new HashSet<Short>();
        for (int i = 0; i < data.length; i++) {
            do {
                data[i] = (short) random.nextInt();
            } while (seen.contains(data[i]));
            seen.add(data[i]);
        }
        return data;
    }


}
