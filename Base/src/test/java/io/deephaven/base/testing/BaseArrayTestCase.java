/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.testing;

abstract public class BaseArrayTestCase extends BaseCachedJMockTestCase {

    public static void assertEquals(String name, final double[][] a, final double[][] b) {
        assertEquals(name, a, b, 1e-10);
    }

    public static void assertEquals(String name, final double[][] a, final double[][] b, final double tolerance) {
        if (a == null || b == null) {
            assertTrue(a == b);
        } else {
            assertTrue(b != null);
            assertEquals(name + ".length", a.length, b.length);
            for (int i = 0; i < a.length; i++) {
                assertEquals(name + "[" + i + "]", a[i], b[i], tolerance);
            }
        }
    }

    public static void assertEquals(final double[][] a, final double[][] b) {
        assertEquals("array", a, b);
    }

    public static void assertEquals(final double[][] a, final double[][] b, final double tolerance) {
        assertEquals("array", a, b, tolerance);
    }

    //
    // boolean[] assertEquals()
    //

    public static void assertEquals(String name, final boolean[] a, final boolean[] b) {
        final String msg = compareArray(name, a, b);
        if (msg != null)
            fail(msg);
    }

    public static void assertEquals(final boolean[] a, final boolean[] b) {
        assertEquals("array", a, b);
    }

    public static void assertEquals(String name, final char[] a, final char[] b) {
        final String msg = compareArray(name, a, b);
        if (msg != null)
            fail(msg);
    }

    public static void assertEquals(final char[] a, final char[] b) {
        assertEquals("array", a, b);
    }

    public static void assertEquals(String name, final byte[] a, final byte[] b) {
        final String msg = compareArray(name, a, b);
        if (msg != null)
            fail(msg);
    }

    public static void assertEquals(final byte[] a, final byte[] b) {
        assertEquals("array", a, b);
    }

    public static void assertEquals(String name, final short[] a, final short[] b) {
        final String msg = compareArray(name, a, b);
        if (msg != null)
            fail(msg);
    }

    public static void assertEquals(final short[] a, final short[] b) {
        assertEquals("array", a, b);
    }

    public static void assertEquals(String name, final int[] a, final int[] b) {
        final String msg = compareArray(name, a, b);
        if (msg != null)
            fail(msg);
    }

    public static void assertEquals(final int[] a, final int[] b) {
        assertEquals("array", a, b);
    }

    //
    // long[] assertEquals()
    //

    public static void assertEquals(String name, final long[] a, final long[] b) {
        final String msg = compareArray(name, a, b);
        if (msg != null)
            fail(msg);
    }

    public static void assertEquals(final long[] a, final long[] b) {
        assertEquals("array", a, b);
    }

    //
    // float[] assertEquals
    //

    public static void assertEquals(String message, final float[] a, final float[] b) {
        assertEquals(message, a, b, 1e-10f);
    }

    public static void assertEquals(final float[] a, final float[] b) {
        assertEquals("array", a, b, 1e-10f);
    }

    public static void assertEquals(final float[] a, final float[] b, float tolerance) {
        assertEquals("array", a, b, tolerance);
    }

    static public void assertEquals(String name, float[] a, float[] b, final float tolerance) {
        final String msg = compareArray(name, a, b, tolerance);
        if (msg != null)
            fail(msg);
    }

    public static void assertCloseInMagnitude(float[] a, float[] b, final float percentage) {
        final String msg = checkMagnitudeArray("array", a, b, percentage);
        if (msg != null)
            fail(msg);
    }

    //
    // double[] assertEquals
    //

    public static void assertEquals(String message, final double[] a, final double[] b) {
        assertEquals(message, a, b, 1e-10);
    }

    public static void assertEquals(final double[] a, final double[] b) {
        assertEquals("array", a, b, 1e-10);
    }

    public static void assertEquals(final double[] a, final double[] b, double tolerance) {
        assertEquals("array", a, b, tolerance);
    }

    static public void assertEquals(String name, double[] a, double[] b, final double tolerance) {
        final String msg = compareArray(name, a, b, tolerance);
        if (msg != null)
            fail(msg);
    }

    public static void assertCloseInMagnitude(double[] a, double[] b, final double percentage) {
        final String msg = checkMagnitudeArray("array", a, b, percentage);
        if (msg != null)
            fail(msg);
    }

    //
    // Object[] assertEquals
    //

    static public void assertEquals(Object[] a, Object[] b) {
        assertEquals("array", a, b);
    }

    static public void assertEquals(final String name, Object[] a, Object[] b) {
        final String msg = compareArray(name, a, b);
        if (null != msg)
            fail(msg);
    }

    public static String compareArray(final String name, final char[] a, final char[] b) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i]);
                if (err != null) {
                    break;
                }
            }
        }
        return err;
    }

    public static String compareArray(final String name, final byte[] a, final byte[] b) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i]);
                if (err != null) {
                    break;
                }
            }
        }
        return err;
    }

    public static String compareArray(final String name, final short[] a, final short[] b) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i]);
                if (err != null)
                    break;
            }
        }
        return err;
    }

    public static String compareArray(final String name, final int[] a, final int[] b) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i]);
                if (err != null)
                    break;
            }
        }
        return err;
    }

    public static String compareArray(final String name, final long[] a, final long[] b) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i]);
                if (err != null)
                    break;
            }
        }
        return err;
    }

    public static String compareArray(final String name, final boolean[] a, final boolean[] b) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i]);
                if (err != null)
                    break;
            }
        }
        return err;
    }

    // Array comparison functions

    public static String compareArray(final String name, final float[] a, final float[] b, float tolerance) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i], tolerance);
                if (err != null)
                    break;
            }
        }
        return err;
    }

    public static String checkMagnitudeArray(final String name, final float[] a, final float[] b, float percentage) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                if (b[i] < (a[i] * (1 - percentage)) || b[i] > (a[i] * (1 + percentage))) {
                    err = "expected " + a[i] + " but was " + b[i];
                }
                if (err != null)
                    break;
            }
        }
        return err;
    }


    public static String compareArray(final String name, final double[] a, final double[] b, double tolerance) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i], tolerance);
                if (err != null)
                    break;
            }
        }
        return err;
    }

    public static String checkMagnitudeArray(final String name, final double[] a, final double[] b, double percentage) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                if (b[i] < (a[i] * (1 - percentage)) || b[i] > (a[i] * (1 + percentage))) {
                    err = "expected " + a[i] + " but was " + b[i];
                }
                if (err != null)
                    break;
            }
        }
        return err;
    }

    static String compareArray(final String name, final Object[] a, final Object[] b) {
        if (a == null) {
            if (b == null) {
                return null;
            } else {
                return name + " expected: null but was: not null";
            }
        } else if (b == null) {
            return name + " expected: not null but was: null";
        }
        String err = null;
        if (a.length != b.length) {
            err = name + " length expected:<" + a.length + "> but was:<" + b.length + ">";
        } else {
            for (int i = 0; i < a.length; i++) {
                err = compare(name + "[" + i + "]", a[i], b[i]);
                if (err != null)
                    break;
            }
        }
        return err;
    }

    /**
     * Compare two objects.
     * 
     * @param name
     * @param ai
     * @param bi
     * @return null if ai.equals(bi) or error message otherwise
     */
    static String compare(final String name, final Object ai, final Object bi) {
        String err = null;
        if (ai == null || bi == null) {
            if (ai != bi) {
                err = xMessage(name, ai, bi);
            }
        } else if (!ai.equals(bi)) {
            err = xMessage(name, ai, bi);
        }
        return err;
    }

    private static String compare(final String name, final long a, final long b) {
        if (a != b) {
            return name + " expected: <" + a + "> but was: <" + b + ">";
        } else {
            return null;
        }
    }

    private static String compare(final String name, final boolean a, final boolean b) {
        if (a != b) {
            return name + " expected: <" + a + "> but was: <" + b + ">";
        } else {
            return null;
        }
    }


    private static String compare(final String name, final double a, final double b, double tolerance) {
        if (Math.abs(a - b) > tolerance || (Double.isNaN(a) ^ Double.isNaN(b))) {
            return name + " expected: <" + a + "> but was: <" + b + ">";
        } else {
            return null;
        }
    }

    /**
     * Construct the error message from an assertion failure
     *
     * @param name
     * @param a
     * @param b
     * @return text error message
     */
    private static String xMessage(final String name, final Object a, final Object b) {
        return name + " expected: " + xname(a) + " but was: " + xname(b);
    }

    private static String xname(Object s) {
        if (s == null) {
            return "null";
        } else {
            return "<" + s + ">";
        }
    }
}
