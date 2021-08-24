package io.deephaven.jpy;

import io.deephaven.jpy.JpyConfig.Flag;
import io.deephaven.util.PrimitiveArrayType;
import io.deephaven.util.PrimitiveArrayType.Booleans;
import io.deephaven.util.PrimitiveArrayType.Bytes;
import io.deephaven.util.PrimitiveArrayType.Chars;
import io.deephaven.util.PrimitiveArrayType.Doubles;
import io.deephaven.util.PrimitiveArrayType.Floats;
import io.deephaven.util.PrimitiveArrayType.Ints;
import io.deephaven.util.PrimitiveArrayType.Longs;
import io.deephaven.util.PrimitiveArrayType.Shorts;
import io.deephaven.util.PrimitiveArrayType.Visitor;
import java.util.EnumSet;
import java.util.Objects;
import org.jpy.PyModule;
import org.jpy.PyObject;

public final class JpyModule implements AutoCloseable {

    public interface ArrayOps<T> {
        PrimitiveArrayType<T> getType();

        PyObject newPyCopy(T values);

        T newCopy(T values);

        PyObject newPyInstance(int len);

        T newInstance(int len);
    }

    private static final String ARRAY_METHOD_NAME = "array";

    public static JpyModule create() {
        return new JpyModule(PyModule.importModule("jpy"));
    }

    private final PyModule module;
    private final PyObject diag;

    JpyModule(PyModule module) {
        this.module = module;
        this.diag = module.getAttribute("diag");
    }

    public int getFlags() {
        return diag.getAttribute("flags", int.class);
    }

    public void setFlags(EnumSet<Flag> flags) {
        int f = 0;
        for (Flag flag : flags) {
            f |= flag.bitset;
        }
        diag.setAttribute("flags", f);
    }

    public <T> ArrayOps<T> ops(PrimitiveArrayType<T> type) {
        return new BaseImpl<>(type);
    }

    @Deprecated
    public PyObject boolean_array(boolean[] values) {
        return ops(Booleans.INSTANCE).newPyCopy(values);
    }

    public boolean[] to_boolean_array(PyObject pyObject) {
        // todo: IDS-6241
        // this is ugly - PyObject.getObjectArrayValue(), or new primitive method, should be able to
        // support this without resorting to jpy specific methods.
        try (final PyObject out = module.call(ARRAY_METHOD_NAME, "boolean", pyObject)) {
            return (boolean[]) out.getObjectValue();
        }
    }

    @Override
    public void close() {
        diag.close();
        module.close();
    }

    class BaseImpl<T> implements ArrayOps<T> {
        private final PrimitiveArrayType<T> type;

        BaseImpl(PrimitiveArrayType<T> type) {
            this.type = Objects.requireNonNull(type, "type");
        }

        @Override
        public PrimitiveArrayType<T> getType() {
            return type;
        }

        @Override
        public PyObject newPyCopy(T values) {
            // PyObject val = jpy.array("<jpy-primitive-type>", existing-array)
            return module.call(
                PyObject.class,
                ARRAY_METHOD_NAME,
                String.class, JpyArrayType.of(type),
                type.getArrayType(), values);
        }

        @Override
        public T newCopy(T values) {
            // primitive-type[] val = jpy.array("<jpy-primitive-type>", existing-array)
            return module.call(
                type.getArrayType(),
                ARRAY_METHOD_NAME,
                String.class, JpyArrayType.of(type),
                type.getArrayType(), values);
        }

        @Override
        public PyObject newPyInstance(int len) {
            // PyObject val = jpy.array("<jpy-primitive-type>", length)
            return module.call(
                PyObject.class,
                ARRAY_METHOD_NAME,
                String.class, JpyArrayType.of(type),
                Integer.class, len);
        }

        @Override
        public T newInstance(int len) {
            // primitive-type[] val = jpy.array("<jpy-primitive-type>", length)
            return module.call(
                type.getArrayType(),
                ARRAY_METHOD_NAME,
                String.class, JpyArrayType.of(type),
                Integer.class, len);
        }
    }

    /**
     * This maps a {@link PrimitiveArrayType} to the corresponding type that jpy expects for an
     * appropriately typed array.
     */
    private static class JpyArrayType implements Visitor {

        static String of(PrimitiveArrayType<?> ops) {
            return Objects.requireNonNull(ops.walk(new JpyArrayType()).out);
        }

        private String out;

        @Override
        public void visit(Booleans booleans) {
            out = "boolean";
        }

        @Override
        public void visit(Bytes bytes) {
            out = "byte";
        }

        @Override
        public void visit(Chars chars) {
            out = "char";
        }

        @Override
        public void visit(Shorts shorts) {
            out = "short";
        }

        @Override
        public void visit(Ints ints) {
            out = "int";
        }

        @Override
        public void visit(Longs longs) {
            out = "long";
        }

        @Override
        public void visit(Floats floats) {
            out = "float";
        }

        @Override
        public void visit(Doubles doubles) {
            out = "double";
        }
    }
}
