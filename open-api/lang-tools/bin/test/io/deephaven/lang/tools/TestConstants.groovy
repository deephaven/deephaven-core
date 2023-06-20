package io.deephaven.lang.tools

import java.util.function.DoubleFunction
import java.util.function.Function
import java.util.function.IntFunction
import java.util.function.LongFunction

/**
 */
trait TestConstants {

    static final double[] ARR_D_2000 = 0d..1999d
    static final double[] ARR_D_2001 = 1d..2000d // it would be valid to write (0d..1999d + 1); valid, but stupid. :-)
    static final float[] ARR_F_2000 = 0f..1999f
    static final float[] ARR_F_2001 = 1f..2000f
    static final long[] ARR_L_2000 = 0L..1999L
    static final long[] ARR_L_2001 = 1L..2000L
    static final int[] ARR_I_2000 = 0..1999
    static final long[] ARR_I_2001 = 1..2000

    static final int[] ARR_D_32 = 0d..31d
    static final int[] ARR_F_32 = 0f..31f
    static final int[] ARR_L_32 = 0L..31L
    static final int[] ARR_I_32 = 0..31
    static final short[] ARR_S_32 = 0..31
    static final char[] ARR_C_32 = 0..31
    static final byte[] ARR_B_32 = 0..31
    static final boolean[] ARR_Z_32 = (0..31).collect{it%3}

    static double[] doubles(double from, double to) {
        new NumberRange(from, to).toArray(new double[0])
    }

    static double[] doubles(double from, double to, double by) {
        new NumberRange(from, to, by).toArray(new double[0])
    }

    static double[] doubles(double from, double to, DoubleFunction<Double> mutate) {
        new NumberRange(from, to).collect(mutate.&apply).toArray(new double[0])
    }

    static List<Object> doublesTo(double from, double to, DoubleFunction<?> mutate) {
        new NumberRange(from, to).collect(mutate.&apply)
    }

    static float[] floats(float from, float to) {
        new NumberRange(from, to)
                .collect({it.floatValue()})
                .toArray(new float[0])
    }

    static float[] floats(float from, float to, float by) {
        new NumberRange(from, to, by)
                .collect({it.floatValue()})
                .toArray(new float[0])
    }

    static float[] floats(float from, float to, Function<Float, Float> mutate) {
        new NumberRange(from, to)
                .collect({it.floatValue()})
                .collect(mutate.&apply)
                .collect({it.floatValue()})
                .toArray(new float[0])
    }
    static List<Object> floatsTo(float from, float to, Function<Float, ?> mutate) {
        new NumberRange(from, to)
                .collect(Double.&floatValue)
                .collect(mutate.&apply)
    }

    static long[] longs(long from, long to) {
        new NumberRange(from, to).toArray(new long[0])
    }

    static long[] longs(long from, long to, long by) {
        new NumberRange(from, to, by).toArray(new long[0])
    }

    static List<Object> longs(long from, long to, LongFunction<?> mutate) {
        new NumberRange(from, to).collect(mutate.&apply)
    }

    static int[] ints(int from, int to) {
        new IntRange(from, to).toArray(new int[0])
    }

    static int[] ints(int from, int to, int by) {
        new IntRange(from, to, by).toArray(new int[0])
    }

    static List<Object> ints(int from, int to, IntFunction<?> mutate) {
        new IntRange(from, to).collect(mutate.&apply)
    }
}
