//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.lang;

import io.deephaven.engine.util.PyCallableWrapper;
import org.jpy.PyObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a dummy implementation of PyCallableWrapper that does not require a Python environment.
 */
public class PyCallableWrapperDummyImpl implements PyCallableWrapper {

    private final List<Class<?>> parameterTypes;
    private boolean vectorizable;

    public PyCallableWrapperDummyImpl(final PyObject obj) {
        throw new UnsupportedOperationException(
                "This constructor only exists to match PyCallableWrapperJpyImpl. Do not use this constructor.");
    }

    public PyCallableWrapperDummyImpl(final List<Class<?>> parameterTypes) {
        this.parameterTypes = Collections.unmodifiableList(parameterTypes);
    }

    @Override
    public PyObject getAttribute(String name) {
        return null;
    }

    @Override
    public <T> T getAttribute(String name, Class<? extends T> valueType) {
        return null;
    }

    @Override
    public void parseSignature() {}

    @Override
    public Object call(Object... args) {
        throw new UnsupportedOperationException();
    }

    public List<Class<?>> getParamTypes() {
        return parameterTypes;
    }

    @Override
    public boolean isVectorized() {
        return false;
    }

    @Override
    public boolean isVectorizable() {
        return vectorizable;
    }

    @Override
    public void setVectorizable(boolean vectorizable) {
        this.vectorizable = vectorizable;
    }

    @Override
    public void initializeChunkArguments() {}

    @Override
    public void addChunkArgument(ChunkArgument ignored) {}

    @Override
    public Signature getSignature() {
        return new Signature(new ArrayList<>(), Void.class);
    }

    @Override
    public void verifyArguments(Class<?>[] argTypes) {

    }

    @Override
    public boolean isVectorizableReturnType() {
        return false;
    }
}
