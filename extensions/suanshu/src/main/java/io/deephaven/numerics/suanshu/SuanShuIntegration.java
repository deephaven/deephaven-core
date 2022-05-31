/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.numerics.suanshu;

import com.google.auto.service.AutoService;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.lang.QueryLibraryImports;
import io.deephaven.vector.*;
import io.deephaven.engine.util.GroovyDeephavenSession;
import com.numericalmethod.suanshu.matrix.MatrixAccessException;
import com.numericalmethod.suanshu.matrix.doubles.Matrix;
import com.numericalmethod.suanshu.matrix.doubles.matrixtype.dense.DenseMatrix;
import com.numericalmethod.suanshu.matrix.doubles.matrixtype.mathoperation.ParallelMatrixMathOperation;
import com.numericalmethod.suanshu.number.Real;
import com.numericalmethod.suanshu.vector.doubles.dense.DenseVector;
import com.numericalmethod.suanshu.vector.doubles.dense.VectorMathOperation;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongToDoubleFunction;
import javax.inject.Inject;

import static io.deephaven.util.QueryConstants.*;


/**
 * Basic utility methods to convert Deephaven data-structures to Suanshu data-structures
 */
public class SuanShuIntegration {

    @AutoService(GroovyDeephavenSession.InitScript.class)
    public static class Script implements GroovyDeephavenSession.InitScript {

        @Inject
        public Script() {}

        @Override
        public String getScriptPath() {
            return "suanshu-init.groovy";
        }

        @Override
        public int priority() {
            return 50;
        }
    }

    @AutoService(QueryLibraryImports.class)
    public static class Imports implements QueryLibraryImports {

        @Override
        public Set<Package> packages() {
            return Collections.emptySet();
        }

        @Override
        public Set<Class<?>> classes() {
            return Collections.emptySet();
        }

        @Override
        public Set<Class<?>> statics() {
            return Collections.singleton(SuanShuIntegration.class);
        }
    }

    private static final int VECTOR_TOSTRING_SIZE = 10;
    private static final int MATRIX__ROW_TOSTRING_SIZE = 3;
    private static final int MATRIX__COLUMN_TOSTRING_SIZE = 3;

    private SuanShuIntegration() {}

    ////////////// Methods to convert Deephaven data-structure to Suanshu data-structures ////////////////


    /**
     * Wraps {@link ByteVector} instance as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param byteVector instance to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by {@link ByteVector}
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final ByteVector byteVector) {
        Require.neqNull(byteVector, "byteVector");
        return new AbstractVectorBaseVector(byteVector) {
            private static final long serialVersionUID = -7281244336713502788L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and {@link ByteVector} is 0-based data-structure,
                // Vector[i] = ByteVector[i-1]
                return getValue(byteVector.get(i - 1));
            }
        };
    }

    /**
     * Wraps <code>byte[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param bytes array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>byte[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final Byte... bytes) {
        Require.neqNull(bytes, "bytes");
        return new AbstractVector() {
            private static final long serialVersionUID = -7356552135900931237L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and byte[] is 0-based data-structure, Vector[i] =
                // ByteVector[i-1]
                return getValue(bytes[i - 1]);
            }

            @Override
            public int size() {
                return bytes.length;
            }
        };
    }

    /**
     * Wraps <code>byte[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param bytes array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>byte[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final byte[] bytes) {
        Require.neqNull(bytes, "bytes");
        return new AbstractVector() {
            private static final long serialVersionUID = -7356552135900931237L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and byte[] is 0-based data-structure, Vector[i] =
                // ByteVector[i-1]
                return getValue(bytes[i - 1]);
            }

            @Override
            public int size() {
                return bytes.length;
            }
        };
    }

    /**
     * Wraps {@link ShortVector} instance as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param shortVector instance to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by {@link ShortVector}
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final ShortVector shortVector) {
        Require.neqNull(shortVector, "shortVector");
        return new AbstractVectorBaseVector(shortVector) {
            private static final long serialVersionUID = -9088059653954005859L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and {@link ShortVector} is 0-based data-structure,
                // Vector[i] = ShortVector[i-1]
                return getValue(shortVector.get(i - 1));
            }
        };
    }

    /**
     * Wraps <code>short[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param shorts array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>short[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final Short... shorts) {
        Require.neqNull(shorts, "shorts");
        return new AbstractVector() {
            private static final long serialVersionUID = -2169099308929428773L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and short[] is 0-based data-structure, Vector[i] =
                // short[i-1]
                return getValue(shorts[i - 1]);
            }

            @Override
            public int size() {
                return shorts.length;
            }
        };
    }

    /**
     * Wraps <code>short[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param shorts array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>short[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final short[] shorts) {
        Require.neqNull(shorts, "shorts");
        return new AbstractVector() {
            private static final long serialVersionUID = -2169099308929428773L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and short[] is 0-based data-structure, Vector[i] =
                // short[i-1]
                return getValue(shorts[i - 1]);
            }

            @Override
            public int size() {
                return shorts.length;
            }
        };
    }

    /**
     * Wraps {@link IntVector} instance as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param intVector instance to wrap
     * @return Immutable{@link Vector} backed by {@link IntVector}
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final IntVector intVector) {
        Require.neqNull(intVector, "intVector");
        return new AbstractVectorBaseVector(intVector) {
            private static final long serialVersionUID = 6372881706069644361L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and {@link IntVector} is 0-based data-structure,
                // Vector[i] = IntVector[i-1]
                return getValue(intVector.get(i - 1));
            }
        };
    }

    /**
     * Wraps <code>int[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param ints array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>int[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final int[] ints) {
        Require.neqNull(ints, "ints");
        return new AbstractVector() {
            private static final long serialVersionUID = -3420295725558692168L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and int[] is 0-based data-structure, Vector[i] =
                // int[i-1]
                return getValue(ints[i - 1]);
            }

            @Override
            public int size() {
                return ints.length;
            }
        };
    }

    /**
     * Wraps <code>int[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param ints array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>int[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final Integer... ints) {
        Require.neqNull(ints, "ints");
        return new AbstractVector() {
            private static final long serialVersionUID = -3420295725558692168L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and int[] is 0-based data-structure, Vector[i] =
                // int[i-1]
                return getValue(ints[i - 1]);
            }

            @Override
            public int size() {
                return ints.length;
            }
        };
    }

    /**
     * Wraps {@link FloatVector} instance as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param floatVector instance to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by {@link FloatVector}
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final FloatVector floatVector) {
        Require.neqNull(floatVector, "floatVector");
        return new AbstractVectorBaseVector(floatVector) {
            private static final long serialVersionUID = 799668019339406883L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and {@link FloatVector} is 0-based data-structure,
                // Vector[i] = FloatVector[i-1]
                return getValue(floatVector.get(i - 1));
            }
        };
    }

    /**
     * Wraps <code>float[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param floats array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>float[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final Float... floats) {
        Require.neqNull(floats, "floats");
        return new AbstractVector() {
            private static final long serialVersionUID = 5421970200304785922L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and float[] is 0-based data-structure, Vector[i] =
                // float[i-1]
                return getValue(floats[i - 1]);
            }

            @Override
            public int size() {
                return floats.length;
            }
        };
    }

    /**
     * Wraps <code>float[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param floats array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>float[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final float[] floats) {
        Require.neqNull(floats, "floats");
        return new AbstractVector() {
            private static final long serialVersionUID = 5421970200304785922L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and float[] is 0-based data-structure, Vector[i] =
                // float[i-1]
                return getValue(floats[i - 1]);
            }

            @Override
            public int size() {
                return floats.length;
            }
        };
    }

    /**
     * Wraps {@link LongVector} instance as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param longVector instance to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by {@link LongVector}
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final LongVector longVector) {
        Require.neqNull(longVector, "longVector");
        return new AbstractVectorBaseVector(longVector) {
            private static final long serialVersionUID = 6215578121732116514L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and {@link LongVector} is 0-based data-structure,
                // Vector[i] = LongVector[i-1]
                return getValue(longVector.get(i - 1));
            }
        };
    }

    /**
     * Wraps <code>long[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param longs array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>long[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final Long... longs) {
        Require.neqNull(longs, "longs");
        return new AbstractVector() {
            private static final long serialVersionUID = -5230174836255083624L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and long[] is 0-based data-structure, Vector[i] =
                // long[i-1]
                return getValue(longs[i - 1]);
            }

            @Override
            public int size() {
                return longs.length;
            }
        };
    }

    /**
     * Wraps <code>long[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param longs array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>long[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final long[] longs) {
        Require.neqNull(longs, "longs");
        return new AbstractVector() {
            private static final long serialVersionUID = -5230174836255083624L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and long[] is 0-based data-structure, Vector[i] =
                // long[i-1]
                return getValue(longs[i - 1]);
            }

            @Override
            public int size() {
                return longs.length;
            }
        };
    }

    /**
     * Wraps {@link DoubleVector} instance as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param doubleVector instance to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by {@link DoubleVector}
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final DoubleVector doubleVector) {
        Require.neqNull(doubleVector, "doubleVector");
        return new AbstractVectorBaseVector(doubleVector) {
            private static final long serialVersionUID = 905559534474469661L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and {@link DoubleVector} is 0-based data-structure,
                // Vector[i] = DoubleVector[i-1]
                return getValue(doubleVector.get(i - 1));
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector deepCopy() {
                return new DenseVector(Arrays.copyOf(this.toArray(), this.size()));
            }
        };
    }

    /**
     * Wraps <code>double[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param doubles array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>double[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final Double... doubles) {
        Require.neqNull(doubles, "doubles");
        return new AbstractVector() {
            private static final long serialVersionUID = 4662277004218374402L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and double[] is 0-based data-structure, Vector[i] =
                // double[i-1]
                return getValue(doubles[i - 1]);
            }

            @Override
            public int size() {
                return doubles.length;
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector deepCopy() {
                return new DenseVector(Arrays.copyOf(this.toArray(), this.size()));
            }
        };
    }

    /**
     * Wraps <code>double[]</code> as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param doubles array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by <code>double[]</code>
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final double[] doubles) {
        Require.neqNull(doubles, "doubles");
        return new AbstractVector() {
            private static final long serialVersionUID = 4662277004218374402L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and double[] is 0-based data-structure, Vector[i] =
                // double[i-1]
                return getValue(doubles[i - 1]);
            }

            @Override
            public int size() {
                return doubles.length;
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector deepCopy() {
                return new DenseVector(Arrays.copyOf(this.toArray(), this.size()));
            }
        };
    }

    /**
     * Wraps {@link Number}[] as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param numbers array to wrap
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by {@link Number}[]
     */
    public static com.numericalmethod.suanshu.vector.doubles.Vector ssVec(final Number... numbers) {
        Require.neqNull(numbers, "numbers");
        return new WrapperArrayBaseVector(numbers);
    }

    /**
     * Wraps {@link ObjectVector} as {@link com.numericalmethod.suanshu.vector.doubles.Vector}
     *
     * @param vector array to wrap
     * @param <T> type of elements in <code>vector</code>
     * @return Immutable {@link com.numericalmethod.suanshu.vector.doubles.Vector} backed by {@link ObjectVector}
     * @throws io.deephaven.base.verify.RequirementFailure if <code>vector</code> contains elements other than
     *         {@link Number} or any of its implementations.
     */
    public static <T extends Number> com.numericalmethod.suanshu.vector.doubles.Vector ssVec(
            final ObjectVector<T> vector) {
        Require.neqNull(vector, "vector");
        Require.requirement(Number.class.isAssignableFrom(vector.getComponentType()),
                "vector of type " + Number.class + ", instead found " + vector.getComponentType());
        return new AbstractVectorBaseVector(vector) {
            private static final long serialVersionUID = 905559534474469661L;

            @Override
            public double get(int i) {
                // Since {@link Vector} is 1-based data-structure and {@link Vector} is 0-based data-structure,
                // Vector[i] = Vector[i-1]
                return getValue(vector.get(i - 1));
            }
        };
    }


    /////////// primitive and wrapper arrays to double array converters /////////////

    /**
     * Converts {@link com.numericalmethod.suanshu.vector.doubles.Vector} to <code>double[]</code>
     *
     * @param vector vector to convert
     * @return converted <code>double[]</code>
     */
    private static double[] convertVectorToDoubleArray(final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
        Require.neqNull(vector, "vector");
        final double[] doubles = new double[vector.size()];
        for (int i = 1; i <= vector.size(); i++) {
            // Since {@link Vector} is 1-based data-structure and double[] is 0-based data-structure, Vector[i] =
            // double[i-1]
            doubles[i - 1] = vector.get(i);
        }
        return doubles;
    }


    ////////////// Deephaven data-structures to Suanshu Matrix ///////////////////


    /**
     * Wraps {@link ByteVector}... as {@link Matrix} This method assumes {@code byteVectors} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param byteVectors array to wrap
     * @return Immutable {@link Matrix} backed by {@link ByteVector}...
     */
    public static Matrix ssMat(final ByteVector... byteVectors) {
        Require.neqNull(byteVectors, "byteVectors");
        for (int i = 0; i < byteVectors.length; i++) {
            Require.neqNull(byteVectors[i], "byteVectors[" + i + "]");
            if (i > 0) {
                Require.eq(byteVectors[0].intSize(), "byteVectors[0].intSize()", byteVectors[i].intSize(),
                        "byteVectors[" + i + "].intSize()");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = 1149204610047946266L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 5811029601409461947L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(byteVectors[i - 1].get(row - 1));
                    }

                    @Override
                    public int size() {
                        return byteVectors.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVectorBaseVector(byteVectors[column - 1]) {// Because 1-based row and column
                                                                              // indices in
                                                                              // com.numericalmethod.suanshu.matrix.doubles.Matrix
                    private static final long serialVersionUID = 6151466803319078752L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(byteVectors[column - 1].get(i - 1));
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(byteVectors[column - 1].get(row - 1));
            }

            @Override
            public int nRows() {
                return byteVectors[0].intSize();
            }

            @Override
            public int nCols() {
                return byteVectors.length;
            }
        };
    }

    /**
     * Wraps <code>byte[]...</code> as {@link Matrix} This method assumes {@code byteColumnsData} to be in
     * unconventional [columns][rows] structure, where first dimension denotes columns and second dimension denotes
     * rows.
     *
     * @param byteColumnsData 2D array to wrap
     * @return Immutable {@link Matrix} backed by <code>byte[]...</code>
     */
    public static Matrix ssMat(final byte[]... byteColumnsData) {
        Require.neqNull(byteColumnsData, "byteColumnsData");
        for (int i = 0; i < byteColumnsData.length; i++) {
            Require.neqNull(byteColumnsData[i], "byteColumnsData[" + i + "]");
            if (i > 0) {
                Require.eq(byteColumnsData[0].length, "byteColumnsData[0].length", byteColumnsData[i].length,
                        "byteColumnsData[" + i + "].length");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = -2397319894087578514L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = -6655135263316645682L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(byteColumnsData[i - 1][row - 1]);
                    }

                    @Override
                    public int size() {
                        return byteColumnsData.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 8776652413193025287L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(byteColumnsData[column - 1][i - 1]);
                    }

                    @Override
                    public int size() {
                        return byteColumnsData[0].length;
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(byteColumnsData[column - 1][row - 1]);
            }

            @Override
            public int nRows() {
                return byteColumnsData[0].length;
            }

            @Override
            public int nCols() {
                return byteColumnsData.length;
            }
        };
    }

    /**
     * Wraps {@link ShortVector}... as {@link Matrix} This method assumes {@code shortVectors} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param shortVectors array to wrap
     * @return Immutable {@link Matrix} backed by {@link ShortVector}...
     */
    public static Matrix ssMat(final ShortVector... shortVectors) {
        Require.neqNull(shortVectors, "shortVectors");
        for (int i = 0; i < shortVectors.length; i++) {
            Require.neqNull(shortVectors[i], "shortVectors[" + i + "]");
            if (i > 0) {
                Require.eq(shortVectors[0].intSize(), "shortVectors[0].intSize()", shortVectors[i].intSize(),
                        "shortVectors[" + i + "].intSize()");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = -2331537155889439961L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 6695958309464803526L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(shortVectors[i - 1].get(row - 1));
                    }

                    @Override
                    public int size() {
                        return shortVectors.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVectorBaseVector(shortVectors[column - 1]) {// Because 1-based row and column
                                                                               // indices in
                                                                               // com.numericalmethod.suanshu.matrix.doubles.Matrix
                    private static final long serialVersionUID = 6991137420725851810L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(shortVectors[column - 1].get(i - 1));
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(shortVectors[column - 1].get(row - 1));
            }

            @Override
            public int nRows() {
                return shortVectors[0].intSize();
            }

            @Override
            public int nCols() {
                return shortVectors.length;
            }
        };
    }

    /**
     * Wraps <code>short[]...</code> as {@link Matrix} This method assumes {@code shortColumnsData} to be in
     * unconventional [columns][rows] structure, where first dimension denotes columns and second dimension denotes
     * rows.
     *
     * @param shortColumnsData 2D array to wrap
     * @return Immutable {@link Matrix} backed by <code>short[]...</code>
     */
    public static Matrix ssMat(final short[]... shortColumnsData) {
        Require.neqNull(shortColumnsData, "shortColumnsData");
        for (int i = 0; i < shortColumnsData.length; i++) {
            Require.neqNull(shortColumnsData[i], "shortColumnsData[" + i + "]");
            if (i > 0) {
                Require.eq(shortColumnsData[0].length, "shortColumnsData[0].length", shortColumnsData[i].length,
                        "shortColumnsData[" + i + "].length");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = 3648623656613668135L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 6000805923325828752L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(shortColumnsData[i - 1][row - 1]);
                    }

                    @Override
                    public int size() {
                        return shortColumnsData.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = -4358292042125326869L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(shortColumnsData[column - 1][i - 1]);
                    }

                    @Override
                    public int size() {
                        return shortColumnsData[0].length;
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(shortColumnsData[column - 1][row - 1]);
            }

            @Override
            public int nRows() {
                return shortColumnsData[0].length;
            }

            @Override
            public int nCols() {
                return shortColumnsData.length;
            }
        };
    }

    /**
     * Wraps {@link IntVector}... as {@link Matrix} This method assumes {@code intVectors} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param intVectors array to wrap
     * @return Immutable {@link Matrix} backed by {@link IntVector}...
     */
    public static Matrix ssMat(final IntVector... intVectors) {
        Require.neqNull(intVectors, "intVectors");
        for (int i = 0; i < intVectors.length; i++) {
            Require.neqNull(intVectors[i], "intVectors[" + i + "]");
            if (i > 0) {
                Require.eq(intVectors[0].intSize(), "intVectors[0].intSize()", intVectors[i].intSize(),
                        "intVectors[" + i + "].intSize()");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = -3165757578289208653L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 3124114667922238415L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(intVectors[i - 1].get(row - 1));
                    }

                    @Override
                    public int size() {
                        return intVectors.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVectorBaseVector(intVectors[column - 1]) {// Because 1-based row and column indices
                                                                             // in
                                                                             // com.numericalmethod.suanshu.matrix.doubles.Matrix
                    private static final long serialVersionUID = 821557745996553552L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(intVectors[column - 1].get(i - 1));
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(intVectors[column - 1].get(row - 1));
            }

            @Override
            public int nRows() {
                return intVectors[0].intSize();
            }

            @Override
            public int nCols() {
                return intVectors.length;
            }
        };
    }

    /**
     * Wraps <code>int[]...</code> as {@link Matrix} This method assumes {@code intColumnsData} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param intColumnsData 2D array to wrap
     * @return Immutable {@link Matrix} backed by <code>int[]...</code>
     */
    public static Matrix ssMat(final int[]... intColumnsData) {
        Require.neqNull(intColumnsData, "intColumnsData");
        for (int i = 0; i < intColumnsData.length; i++) {
            Require.neqNull(intColumnsData[i], "intColumnsData[" + i + "]");
            if (i > 0) {
                Require.eq(intColumnsData[0].length, "intColumnsData[0].length", intColumnsData[i].length,
                        "intColumnsData[" + i + "].length");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = -2331343961789969900L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 5920186710526702399L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(intColumnsData[i - 1][row - 1]);
                    }

                    @Override
                    public int size() {
                        return intColumnsData.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = -8323652796518072916L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(intColumnsData[column - 1][i - 1]);
                    }

                    @Override
                    public int size() {
                        return intColumnsData[0].length;
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(intColumnsData[column - 1][row - 1]);
            }

            @Override
            public int nRows() {
                return intColumnsData[0].length;
            }

            @Override
            public int nCols() {
                return intColumnsData.length;
            }
        };
    }

    /**
     * Wraps {@link FloatVector}... as {@link Matrix} This method assumes {@code floatVectors} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param floatVectors array to wrap
     * @return Immutable {@link Matrix} backed by {@link FloatVector}...
     */
    public static Matrix ssMat(final FloatVector... floatVectors) {
        Require.neqNull(floatVectors, "floatVectors");
        for (int i = 0; i < floatVectors.length; i++) {
            Require.neqNull(floatVectors[i], "floatVectors[" + i + "]");
            if (i > 0) {
                Require.eq(floatVectors[0].intSize(), "floatVectors[0].intSize()", floatVectors[i].intSize(),
                        "floatVectors[" + i + "].intSize()");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = -3144866921663267643L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 2773539420255792152L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(floatVectors[i - 1].get(row - 1));
                    }

                    @Override
                    public int size() {
                        return floatVectors.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVectorBaseVector(floatVectors[column - 1]) {// Because 1-based row and column
                                                                               // indices in
                                                                               // com.numericalmethod.suanshu.matrix.doubles.Matrix
                    private static final long serialVersionUID = -8535605234772136511L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(floatVectors[column - 1].get(i - 1));
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(floatVectors[column - 1].get(row - 1));
            }

            @Override
            public int nRows() {
                return floatVectors[0].intSize();
            }

            @Override
            public int nCols() {
                return floatVectors.length;
            }
        };
    }

    /**
     * Wraps <code>float[]...</code> as {@link Matrix}. This method assumes {@code floatColumnsData} to be in
     * unconventional [columns][rows] structure, where first dimension denotes columns and second dimension denotes
     * rows.
     *
     * @param floatColumnsData 2D array to wrap
     * @return Immutable {@link Matrix} backed by <code>float[]...</code>
     */
    public static Matrix ssMat(final float[]... floatColumnsData) {
        Require.neqNull(floatColumnsData, "floatColumnsData");
        for (int i = 0; i < floatColumnsData.length; i++) {
            Require.neqNull(floatColumnsData[i], "floatColumnsData[" + i + "]");
            if (i > 0) {
                Require.eq(floatColumnsData[0].length, "floatColumnsData[0].length", floatColumnsData[i].length,
                        "floatColumnsData[" + i + "].length");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = 8545232805676960973L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = -2152835767610313213L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(floatColumnsData[i - 1][row - 1]);
                    }

                    @Override
                    public int size() {
                        return floatColumnsData.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 6874105163141506182L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(floatColumnsData[column - 1][i - 1]);
                    }

                    @Override
                    public int size() {
                        return floatColumnsData[0].length;
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(floatColumnsData[column - 1][row - 1]);
            }

            @Override
            public int nRows() {
                return floatColumnsData[0].length;
            }

            @Override
            public int nCols() {
                return floatColumnsData.length;
            }
        };
    }

    /**
     * Wraps {@link LongVector}... as {@link Matrix} This method assumes {@code longVectors} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param longVectors array to wrap
     * @return Immutable {@link Matrix} backed by {@link LongVector}...
     */
    public static Matrix ssMat(final LongVector... longVectors) {
        Require.neqNull(longVectors, "longVectors");
        for (int i = 0; i < longVectors.length; i++) {
            Require.neqNull(longVectors[i], "longVectors[" + i + "]");
            if (i > 0) {
                Require.eq(longVectors[0].intSize(), "longVectors[0].intSize()", longVectors[i].intSize(),
                        "longVectors[" + i + "].intSize()");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = -2717218802875838966L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 7749544930085654412L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(longVectors[i - 1].get(row - 1));
                    }

                    @Override
                    public int size() {
                        return longVectors.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVectorBaseVector(longVectors[column - 1]) {// Because 1-based row and column
                                                                              // indices in
                                                                              // com.numericalmethod.suanshu.matrix.doubles.Matrix
                    private static final long serialVersionUID = 4391740406197864817L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(longVectors[column - 1].get(i - 1));
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(longVectors[column - 1].get(row - 1));
            }

            @Override
            public int nRows() {
                return longVectors[0].intSize();
            }

            @Override
            public int nCols() {
                return longVectors.length;
            }
        };
    }

    /**
     * Wraps <code>long[]...</code> as {@link Matrix} This method assumes {@code longColumnsData} to be in
     * unconventional [columns][rows] structure, where first dimension denotes columns and second dimension denotes
     * rows.
     *
     * @param longColumnsData 2D array to wrap
     * @return Immutable {@link Matrix} backed by <code>long[]...</code>
     */
    public static Matrix ssMat(final long[]... longColumnsData) {
        Require.neqNull(longColumnsData, "longColumnsData");
        for (int i = 0; i < longColumnsData.length; i++) {
            Require.neqNull(longColumnsData[i], "longColumnsData[" + i + "]");
            if (i > 0) {
                Require.eq(longColumnsData[0].length, "longColumnsData[0].length", longColumnsData[i].length,
                        "longColumnsData[" + i + "].length");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = 6495688465302901272L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = -9005154733650532921L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(longColumnsData[i - 1][row - 1]);
                    }

                    @Override
                    public int size() {
                        return longColumnsData.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = -3740303268339723983L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(longColumnsData[column - 1][i - 1]);
                    }

                    @Override
                    public int size() {
                        return longColumnsData[0].length;
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(longColumnsData[column - 1][row - 1]);
            }

            @Override
            public int nRows() {
                return longColumnsData[0].length;
            }

            @Override
            public int nCols() {
                return longColumnsData.length;
            }
        };
    }

    /**
     * Wraps {@link DoubleVector}... as {@link Matrix} This method assumes {@code doubleVectors} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param doubleVectors array to wrap
     * @return Immutable {@link Matrix} backed by {@link DoubleVector}...
     */
    public static Matrix ssMat(final DoubleVector... doubleVectors) {
        Require.neqNull(doubleVectors, "doubleVectors");
        for (int i = 0; i < doubleVectors.length; i++) {
            Require.neqNull(doubleVectors[i], "doubleVectors[" + i + "]");
            if (i > 0) {
                Require.eq(doubleVectors[0].intSize(), "doubleVectors[0].intSize()", doubleVectors[i].intSize(),
                        "doubleVectors[" + i + "].intSize()");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = -7698508338229085425L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 6948716975163062302L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(doubleVectors[i - 1].get(row - 1));
                    }

                    @Override
                    public int size() {
                        return doubleVectors.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVectorBaseVector(doubleVectors[column - 1]) {// Because 1-based row and column
                                                                                // indices in
                                                                                // com.numericalmethod.suanshu.matrix.doubles.Matrix
                    private static final long serialVersionUID = 172294086541855763L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(doubleVectors[column - 1].get(i - 1));
                    }

                    @Override
                    public com.numericalmethod.suanshu.vector.doubles.Vector deepCopy() {
                        return new DenseVector(Arrays.copyOf(this.toArray(), this.size()));
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(doubleVectors[column - 1].get(row - 1));
            }

            @Override
            public int nRows() {
                return doubleVectors[0].intSize();
            }

            @Override
            public int nCols() {
                return doubleVectors.length;
            }
        };
    }

    /**
     * Wraps <code>double[]...</code> as {@link Matrix} This method assumes {@code doubleColumnsData} to be in
     * unconventional [columns][rows] structure, where first dimension denotes columns and second dimension denotes
     * rows.
     *
     * @param doubleColumnsData 2D array to wrap
     * @return Immutable {@link Matrix} backed by <code>double[]...</code>
     */
    public static Matrix ssMat(final double[]... doubleColumnsData) {
        Require.neqNull(doubleColumnsData, "doubleColumnsData");
        for (int i = 0; i < doubleColumnsData.length; i++) {
            Require.neqNull(doubleColumnsData[i], "doubleColumnsData[" + i + "]");
            if (i > 0) {
                Require.eq(doubleColumnsData[0].length, "doubleColumnsData[0].length", doubleColumnsData[i].length,
                        "doubleColumnsData[" + i + "].length");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = 10613528742337804L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 5945640384881789872L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(doubleColumnsData[i - 1][row - 1]);
                    }

                    @Override
                    public int size() {
                        return doubleColumnsData.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = 2519265445719875525L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(doubleColumnsData[column - 1][i - 1]);
                    }

                    @Override
                    public int size() {
                        return doubleColumnsData[0].length;
                    }

                    @Override
                    public com.numericalmethod.suanshu.vector.doubles.Vector deepCopy() {
                        return new DenseVector(Arrays.copyOf(this.toArray(), this.size()));
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(doubleColumnsData[column - 1][row - 1]);
            }

            @Override
            public int nRows() {
                return doubleColumnsData[0].length;
            }

            @Override
            public int nCols() {
                return doubleColumnsData.length;
            }
        };
    }

    /**
     * Wrap {@link Number}[]... as {@link Matrix} This method assumes {@code numberColumnsData} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param numberColumnsData 2D array to wrap
     * @return Immutable {@link Matrix} backed by {@link Number}[]...
     */
    public static Matrix ssMat(final Number[]... numberColumnsData) {
        Require.neqNull(numberColumnsData, "numberColumnsData");
        for (int i = 0; i < numberColumnsData.length; i++) {
            Require.neqNull(numberColumnsData[i], "numberColumnsData[" + i + "]");
            if (i > 0) {
                Require.eq(numberColumnsData[0].length, "numberColumnsData[0].length", numberColumnsData[i].length,
                        "numberColumnsData[" + i + "].length");
            }
        }
        return new AbstractMatrix() {
            private static final long serialVersionUID = -2313696318996931299L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = -8228534644613258977L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return getValue(numberColumnsData[i - 1][row - 1]);
                    }

                    @Override
                    public int size() {
                        return numberColumnsData.length;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                    throws MatrixAccessException {
                return new WrapperArrayBaseVector(numberColumnsData[column - 1]);
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return getValue(numberColumnsData[column - 1][row - 1]);
            }

            @Override
            public int nRows() {
                return numberColumnsData[0].length;
            }

            @Override
            public int nCols() {
                return numberColumnsData.length;
            }
        };
    }

    private static Optional<LongToDoubleFunction> makeDoubleAccessor(@NotNull final Vector<?> vector) {
        final LongToDoubleFunction accessor;
        if (vector instanceof DoubleVector) {
            accessor = (final long pos) -> getValue(((DoubleVector) vector).get(pos));
        } else if (vector instanceof LongVector) {
            accessor = (final long pos) -> getValue(((LongVector) vector).get(pos));
        } else if (vector instanceof FloatVector) {
            accessor = (final long pos) -> getValue(((FloatVector) vector).get(pos));
        } else if (vector instanceof IntVector) {
            accessor = (final long pos) -> getValue(((IntVector) vector).get(pos));
        } else if (vector instanceof ShortVector) {
            accessor = (final long pos) -> getValue(((ShortVector) vector).get(pos));
        } else if (vector instanceof ByteVector) {
            accessor = (final long pos) -> getValue(((ByteVector) vector).get(pos));
        } else if (vector instanceof ObjectVector && Number.class.isAssignableFrom(vector.getComponentType())) {
            // noinspection unchecked
            accessor = (final long pos) -> getValue(((ObjectVector<? extends Number>) vector).get(pos));
        } else {
            accessor = null;
        }
        return Optional.ofNullable(accessor);
    }

    /**
     * Wraps {@link Vector}... as {@link Matrix} This method assumes {@code vectors} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param vectors array to wrap
     * @return Immutable {@link Matrix} backed by {@link Vector}...
     * @throws UnsupportedOperationException if any of the arrays in {@code vectors} does not belong to
     *         {{@link ByteVector}, {@link ShortVector}, {@link IntVector}, {@link FloatVector}, {@link LongVector},
     *         {@link DoubleVector}, {@link ObjectVector}<code>&lt;? extends {@link Number}&gt;</code>}
     */
    public static Matrix ssMat(final Vector... vectors) {
        return ssMat(new ObjectVectorDirect<>(vectors));
    }

    /**
     * Wraps {@link ObjectVector}... as {@link Matrix} This method assumes {@code dhVector} to be in unconventional
     * [columns][rows] structure, where first dimension denotes columns and second dimension denotes rows.
     *
     * @param objectVector vector to wrap
     * @param <T> - type of elements in <code>dhVector</code>
     * @return Immutable {@link Matrix} backed by {@link ObjectVector}...
     * @throws UnsupportedOperationException if any of the vectors in {@code objectVector} does not belong to
     *         {{@link ByteVector}, {@link ShortVector}, {@link IntVector}, {@link FloatVector}, {@link LongVector},
     *         {@link DoubleVector}, {@link ObjectVector}<code>&lt;? extends {@link Number}&gt;</code>}
     */
    public static <T extends Vector> Matrix ssMat(final ObjectVector<T> objectVector) {
        Require.neqNull(objectVector, "objectVector");
        final int nCols = objectVector.intSize();
        final int nRows = objectVector.isEmpty() ? 0 : objectVector.get(0).intSize();
        final LongToDoubleFunction[] accessors = new LongToDoubleFunction[nCols];
        for (int ai = 0; ai < nCols; ai++) {
            final Vector<?> vector = objectVector.get(ai);
            if (vector == null) {
                throw new IllegalArgumentException("Null array at rowSet " + ai);
            }
            if (ai > 0 && vector.intSize() != nRows) {
                throw new IllegalArgumentException("Size mismatch: first array has size " + nRows + ", array at rowSet "
                        + ai + " has size " + vector.intSize());
            }
            final int arrayIndex = ai;
            accessors[ai] = makeDoubleAccessor(vector)
                    .orElseThrow(() -> new UnsupportedOperationException(
                            "Invalid array at rowSet " + arrayIndex + " with type " + vector.getClass()
                                    + " and component type " + vector.getComponentType() + ": must be numeric"));
        }

        return new AbstractMatrix() {
            private static final long serialVersionUID = 1468546253357645902L;

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException {
                return new AbstractVector() {
                    private static final long serialVersionUID = -7067215087902513883L;

                    @Override
                    public double get(int i) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return accessors[i - 1].applyAsDouble(row - 1);
                    }

                    @Override
                    public int size() {
                        return nCols;
                    }
                };
            }

            @Override
            public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(final int column)
                    throws MatrixAccessException {
                return new AbstractVectorBaseVector(objectVector.get(column - 1)) {
                    private static final long serialVersionUID = 8517809020282279391L;

                    @Override
                    public double get(final int row) {
                        // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                        return accessors[column - 1].applyAsDouble(row - 1);
                    }
                };
            }

            @Override
            public double get(final int row, final int column) throws MatrixAccessException {
                // Because 1-based row and column indices in com.numericalmethod.suanshu.matrix.doubles.Matrix
                return accessors[column - 1].applyAsDouble(row - 1);
            }

            @Override
            public int nRows() {
                return nRows;
            }

            @Override
            public int nCols() {
                return nCols;
            }
        };
    }


    ////////////// Value getters that handles null as well as QueryConstants.NULL_* ///////////////


    private static double getValue(final byte value) {
        return value == NULL_BYTE ? Double.NaN : value;
    }

    private static double getValue(final short value) {
        return value == NULL_SHORT ? Double.NaN : value;
    }

    private static double getValue(final int value) {
        return value == NULL_INT ? Double.NaN : value;
    }

    private static double getValue(final float value) {
        return value == NULL_FLOAT ? Double.NaN : value;
    }

    private static double getValue(final long value) {
        return value == NULL_LONG ? Double.NaN : value;
    }

    private static double getValue(final double value) {
        return value == NULL_DOUBLE ? Double.NaN : value;
    }

    private static <T extends Number> double getValue(final T value) {
        return (value == null || (Byte.class.isAssignableFrom(value.getClass()) && value.equals(NULL_BYTE))
                || (Short.class.isAssignableFrom(value.getClass()) && value.equals(NULL_SHORT))
                || (Integer.class.isAssignableFrom(value.getClass()) && value.equals(NULL_INT))
                || (Float.class.isAssignableFrom(value.getClass()) && value.equals(NULL_FLOAT))
                || (Long.class.isAssignableFrom(value.getClass()) && value.equals(NULL_LONG))
                || (Double.class.isAssignableFrom(value.getClass()) && value.equals(NULL_DOUBLE)))
                        ? Double.NaN
                        : value.doubleValue();
    }


    /**
     * The abstract implementation of {@link com.numericalmethod.suanshu.vector.doubles.Vector}.
     */
    public abstract static class AbstractVector
            implements com.numericalmethod.suanshu.vector.doubles.Vector, Serializable {

        private static final long serialVersionUID = -7713580887929399868L;

        /**
         * Gets the i<sup>th</sup> indexed (1-based) value from vector.
         *
         * @param i 1-based index
         * @return i<sup>th</sup> indexed (1-based) value
         */
        @Override
        public abstract double get(final int i);

        @Override
        public void set(final int index, final double value) {
            throw new UnsupportedOperationException("Setting elements for vectors is not supported.");
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector add(
                final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
            return new ImmutableVector(new VectorMathOperation().add(this, vector));
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector minus(
                final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
            return new ImmutableVector(new VectorMathOperation().minus(this, vector));
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector multiply(
                final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
            return new ImmutableVector(new VectorMathOperation().multiply(this, vector));
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector divide(
                final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
            return new ImmutableVector(new VectorMathOperation().divide(this, vector));
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector add(final double v) {
            return new ImmutableVector(new VectorMathOperation().add(this, v));
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector minus(final double v) {
            return new ImmutableVector(new VectorMathOperation().minus(this, v));
        }

        @Override
        public double innerProduct(final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
            return new VectorMathOperation().innerProduct(this, vector);
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector pow(final double v) {
            return new ImmutableVector(new VectorMathOperation().pow(this, v));
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector scaled(final double v) {
            return new ImmutableVector(new VectorMathOperation().scaled(this, v));
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector scaled(final Real real) {
            return new ImmutableVector(new VectorMathOperation().scaled(this, real));
        }

        @Override
        public double norm() {
            return new VectorMathOperation().norm(this);
        }

        @Override
        public double norm(final int i) {
            return new VectorMathOperation().norm(this, i);
        }

        @Override
        public double angle(final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
            return new VectorMathOperation().angle(this, vector);
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector opposite() {
            return new ImmutableVector(new VectorMathOperation().opposite(this));
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector ZERO() {
            return new PrimitiveDoubleArrayBaseVector(new double[this.size()]);
        }

        @Override
        public double[] toArray() {
            return convertVectorToDoubleArray(this);
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector deepCopy() {
            return new DenseVector(this);
        }

        /**
         * Returns the compact {@link String} representation of
         * {@link com.numericalmethod.suanshu.vector.doubles.Vector}. If you want to have String representation of the
         * whole {@link com.numericalmethod.suanshu.vector.doubles.Vector}, please use {@code show()} method.
         *
         * @return Compact string representation of {@link com.numericalmethod.suanshu.vector.doubles.Vector}
         */
        @Override
        public String toString() {
            return show(VECTOR_TOSTRING_SIZE);
        }

        /**
         * Returns the {@link String} representation of whole {@link com.numericalmethod.suanshu.vector.doubles.Vector}
         *
         * @return String representation of {@link com.numericalmethod.suanshu.vector.doubles.Vector}
         */
        public String show() {
            return show(size());
        }

        private String show(final int size) {
            final int sizeToShow = size < 0 ? size() : Math.min(size, size());
            final StringBuilder strToReturn = new StringBuilder();
            strToReturn.append("[");

            for (int i = 1; i <= sizeToShow; ++i) {
                strToReturn.append(String.format("%f, ", get(i)));
            }

            if (sizeToShow < size()) {
                strToReturn.append("...]");
                return strToReturn.toString();
            }
            strToReturn.setCharAt(strToReturn.length() - 2, ']');
            return strToReturn.toString();
        }
    }


    private static class ImmutableVector extends AbstractVector {

        private static final long serialVersionUID = -3788576370567706215L;
        private final com.numericalmethod.suanshu.vector.doubles.Vector vector;

        private ImmutableVector(final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
            this.vector = vector;
        }

        @Override
        public int size() {
            return vector.size();
        }

        @Override
        public double get(int i) {
            return vector.get(i);
        }
    }

    private abstract static class AbstractVectorBaseVector extends AbstractVector {

        private static final long serialVersionUID = -8693469432136886358L;
        private final Vector vector;

        private AbstractVectorBaseVector(final Vector vector) {
            this.vector = vector;
        }

        @Override
        public int size() {
            return vector.intSize();
        }
    }

    private static class PrimitiveDoubleArrayBaseVector extends AbstractVector {

        private static final long serialVersionUID = -2276083865006961223L;

        private final double[] doubles;

        private PrimitiveDoubleArrayBaseVector(final double[] doubles) {
            this.doubles = doubles;
        }

        @Override
        public int size() {
            return doubles.length;
        }

        @Override
        public double get(int i) {
            return getValue(doubles[i - 1]);
        }

        @Override
        public double[] toArray() {
            return Arrays.copyOf(doubles, doubles.length);
        }
    }

    private static class WrapperArrayBaseVector extends AbstractVector {

        private static final long serialVersionUID = -5975412464895986098L;
        final Number[] nums;

        WrapperArrayBaseVector(final Number[] nums) {
            this.nums = nums;
        }

        @Override
        public int size() {
            return nums.length;
        }

        @Override
        public double get(final int i) {
            // Since {@link Vector} is 1-based data-structure and Number[] is 0-based data-structure, Vector[i] =
            // Number[i-1]
            return getValue(nums[i - 1]);
        }
    }


    /**
     * The abstract implementation of {@link Matrix}.
     */
    public abstract static class AbstractMatrix implements Matrix, Serializable {

        private static final long serialVersionUID = 1940714674230668397L;

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector multiply(
                final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
            return new ImmutableVector(new ParallelMatrixMathOperation().multiply(this, vector));
        }

        /**
         * Gets the row<sup>th</sup> indexed (1-based) row-vector from matrix.
         *
         * @param row 1-based index
         * @return vector at row<sup>th</sup> indexed (1-based)
         */
        @Override
        public abstract com.numericalmethod.suanshu.vector.doubles.Vector getRow(int row) throws MatrixAccessException;

        /**
         * Gets the column<sup>th</sup> indexed (1-based) column-vector from matrix.
         *
         * @param column 1-based index
         * @return vector at column<sup>th</sup> indexed (1-based)
         */
        @Override
        public abstract com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int column)
                throws MatrixAccessException;

        /**
         * Gets the value at row<sup>th</sup> indexed row (1-based) and column<sup>th</sup> indexed (1-based) column
         * from matrix.
         *
         * @param row 1-based row-index
         * @param column 1-based column-index
         * @return value at row<sup>th</sup> indexed row (1-based) and column<sup>th</sup> indexed (1-based) column
         */
        @Override
        public abstract double get(final int row, final int column) throws MatrixAccessException;

        @Override
        public Matrix scaled(final double v) {
            return new ImmutableMatrix(new ParallelMatrixMathOperation().scaled(this, v));
        }

        @Override
        public Matrix deepCopy() {
            return new DenseMatrix(this);
        }

        @Override
        public void set(final int row, final int column, final double value) throws MatrixAccessException {
            throw new UnsupportedOperationException("Setting elements for matrix is not supported.");
        }

        @Override
        public Matrix t() {
            return new ImmutableMatrix(new ParallelMatrixMathOperation().transpose(this));
        }

        @Override
        public Matrix add(final Matrix matrix) {
            return new ImmutableMatrix(new ParallelMatrixMathOperation().add(this, matrix));
        }

        @Override
        public Matrix minus(final Matrix matrix) {
            return new ImmutableMatrix(new ParallelMatrixMathOperation().minus(this, matrix));
        }

        @Override
        public Matrix multiply(final Matrix matrix) {
            return new ImmutableMatrix(new ParallelMatrixMathOperation().multiply(this, matrix));
        }

        @Override
        public Matrix opposite() {
            return this.scaled(-1.0D);
        }

        @Override
        public Matrix ZERO() {
            return new ImmutableMatrix(new DenseMatrix(nRows(), nCols()));
        }

        @Override
        public Matrix ONE() {
            return new ImmutableMatrix(new DenseMatrix(nRows(), nCols()).ONE());
        }

        /**
         * Returns the compact {@link String} representation of {@link Matrix}. If you want to have String
         * representation of the whole {@link Matrix}, please use {@code show()} method.
         *
         * @return Compact string representation of {@link Matrix}
         */
        @Override
        public String toString() {
            return show(MATRIX__ROW_TOSTRING_SIZE, MATRIX__COLUMN_TOSTRING_SIZE);
        }

        /**
         * Returns the {@link String} representation of whole {@link Matrix}
         *
         * @return String representation of {@link Matrix}
         */
        public String show() {
            return show(this.nRows(), this.nCols());
        }

        private String show(final int maxRows, final int maxCols) {
            final int rowSizeToShow = maxRows < 0 ? this.nRows() : Math.min(maxRows, this.nRows());
            final int columnSizeToShow = maxCols < 0 ? this.nCols() : Math.min(maxCols, this.nCols());

            final StringBuilder result = new StringBuilder();
            result.append(String.format("%dx%d\n", this.nRows(), this.nCols()));
            result.append("\t");

            int i;
            for (i = 1; i <= columnSizeToShow; ++i) {
                result.append(String.format("[,%d] ", i));
            }
            if (columnSizeToShow < this.nCols()) {
                result.append("...");
            }

            result.append("\n");

            for (i = 1; i <= rowSizeToShow; ++i) {
                result.append(String.format("[%d,] ", i));

                for (int j = 1; j <= columnSizeToShow; ++j) {
                    result.append(String.format("%f, ", this.get(i, j)));
                }

                if (i != this.nRows()) {
                    result.append(columnSizeToShow < this.nCols() ? "...\n" : "\n");
                }
            }

            if (rowSizeToShow < this.nRows()) {
                result.append(".  ...\n.  ...\n.  ...\n");
            }

            return result.toString();
        }
    }

    private static class ImmutableMatrix extends AbstractMatrix {

        private static final long serialVersionUID = -1607409347664311905L;
        private final Matrix matrix;

        private ImmutableMatrix(final Matrix matrix) {
            this.matrix = matrix;
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector getRow(int i) throws MatrixAccessException {
            return matrix.getRow(i);
        }

        @Override
        public com.numericalmethod.suanshu.vector.doubles.Vector getColumn(int i) throws MatrixAccessException {
            return matrix.getColumn(i);
        }

        @Override
        public double get(int row, int column) throws MatrixAccessException {
            return matrix.get(row, column);
        }

        @Override
        public int nRows() {
            return matrix.nRows();
        }

        @Override
        public int nCols() {
            return matrix.nCols();
        }
    }
}
