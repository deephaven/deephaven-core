package io.deephaven.engine.util;

import org.jpy.PyObject;

import java.util.List;

/**
 * Created by rbasralian on 8/12/23
 */
public interface PyCallableWrapper {
    PyObject getAttribute(String name);

    <T> T getAttribute(String name, Class<? extends T> valueType);

    void parseSignature();

    Object call(Object... args);

    List<Class<?>> getParamTypes();

    boolean isVectorized();

    boolean isVectorizable();

    void setVectorizable(boolean vectorizable);

    void initializeChunkArguments();

    void addChunkArgument(ChunkArgument chunkArgument);

    Class<?> getReturnType();

    abstract class ChunkArgument {
        private final Class<?> type;

        public Class<?> getType() {
            return type;
        }

        public ChunkArgument(Class<?> type) {
            this.type = type;
        }
    }

    class ColumnChunkArgument extends ChunkArgument {

        private final String columnName;
        private int sourceChunkIndex;
        private boolean resolved = false;

        public ColumnChunkArgument(String columnName, Class<?> type) {
            super(type);
            this.columnName = columnName;
        }

        public void setSourceChunkIndex(int sourceChunkIndex) {
            this.resolved = true;
            this.sourceChunkIndex = sourceChunkIndex;
        }

        public int getSourceChunkIndex() {
            if (!resolved) {
                throw new IllegalStateException(
                        "The column chunk argument for " + columnName + " hasn't been resolved");
            }
            return sourceChunkIndex;
        }

        public String getColumnName() {
            return columnName;
        }
    }

    class ConstantChunkArgument extends ChunkArgument {
        private final Object value;

        public ConstantChunkArgument(Object value, Class<?> type) {
            super(type);
            this.value = value;
        }

        public Object getValue() {
            return value;
        }
    }

    boolean isVectorizableReturnType();
}
