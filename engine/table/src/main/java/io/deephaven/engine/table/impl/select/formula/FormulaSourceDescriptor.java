package io.deephaven.engine.table.impl.select.formula;

public class FormulaSourceDescriptor {
    public final Class returnType;
    public final String[] sources;
    public final String[] arrays;
    public final String[] params;

    public FormulaSourceDescriptor(Class returnType, String[] sources, String[] arrays, String[] params) {
        this.returnType = returnType;
        this.sources = sources;
        this.arrays = arrays;
        this.params = params;
    }
}
